//! Password hashes: PBKDF2-HMAC-SHA256, written as PHC strings.
//!
//! A hash reads `$pbkdf2-sha256$i=600000,l=32$<salt>$<hash>`: the algorithm, the iteration count
//! `i`, the optional length of the hash `l`, then the salt and the hash in base64 without
//! padding, as the PHC string format writes them
//! (<https://github.com/P-H-C/phc-string-format>).
//!
//! PBKDF2 rather than Argon2id: it runs on aws-lc-rs, the one crypto provider OpenQTT already
//! links, where Argon2id would bring a second implementation of cryptography into the broker,
//! and it costs CPU only. An Argon2id verification at the recommended parameters holds 19 MiB
//! for its duration, and an edge verifies a burst of reconnecting clients side by side.
//! PBKDF2 is the weaker against GPUs; the iteration count, 600,000 by default (the OWASP
//! recommendation of 2023), is what pays for that, and is kept in each hash so it can rise
//! later without invalidating the hashes already written.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use aws_lc_rs::pbkdf2;
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD_NO_PAD as B64;

use crate::Error;

/// The PHC identifier of the one algorithm.
pub const ALGORITHM: &str = "pbkdf2-sha256";

/// The iteration count of a hash this crate makes.
pub const DEFAULT_ITERATIONS: u32 = 600_000;

/// The fewest iterations a hash may have. Below this a hash protects little once it leaks.
pub const MIN_ITERATIONS: u32 = 100_000;

/// The most iterations a hash may have, so that no line of a list can make every check of its
/// user take minutes.
pub const MAX_ITERATIONS: u32 = 10_000_000;

/// The salt of a hash this crate makes, in bytes: 128 bits, as NIST SP 800-132 asks.
const SALT_LEN: usize = 16;

/// The hash of a hash this crate makes, in bytes: one SHA-256 output.
const HASH_LEN: usize = 32;

/// The sizes a salt or a hash read from a PHC string may have, in bytes.
const MIN_PART: usize = 16;
const MAX_PART: usize = 64;

/// A password hash: PBKDF2-HMAC-SHA256 with an iteration count, a salt and the derived key.
///
/// Its `Debug` shows the iteration count only: a hash is not the password, but it is what an
/// attacker would guess against offline.
#[derive(Clone, PartialEq, Eq)]
pub struct PasswordHash {
    iterations: NonZeroU32,
    salt: Box<[u8]>,
    hash: Box<[u8]>,
}

impl PasswordHash {
    /// Reads a PHC string.
    ///
    /// # Errors
    ///
    /// [`Error::PasswordHash`] when it names another algorithm or a parameter other than `i`
    /// and `l`, has an iteration count outside [`MIN_ITERATIONS`] to [`MAX_ITERATIONS`], or a
    /// salt or hash that is not base64 without padding of 16 to 64 bytes.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let fail = |reason| Error::PasswordHash { reason };
        let mut parts = text.split('$');
        if parts.next() != Some("") {
            return Err(fail("it does not begin with `$`"));
        }
        if parts.next() != Some(ALGORITHM) {
            return Err(fail("the algorithm is not pbkdf2-sha256"));
        }
        let (Some(params), Some(salt), Some(hash), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(fail("it does not have four `$`-separated fields"));
        };
        let mut iterations = None;
        let mut length = None;
        for param in params.split(',') {
            let (name, value) = param
                .split_once('=')
                .ok_or_else(|| fail("a parameter is not `name=value`"))?;
            let slot = match name {
                "i" => &mut iterations,
                "l" => &mut length,
                _ => return Err(fail("a parameter other than `i` and `l`")),
            };
            if slot.is_some() {
                return Err(fail("a parameter appears twice"));
            }
            *slot = Some(decimal(value).ok_or_else(|| fail("a parameter is not a number"))?);
        }
        let iterations = iterations.ok_or_else(|| fail("the iteration count `i` is missing"))?;
        if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&iterations) {
            return Err(fail(
                "the iteration count is below 100,000 or above 10,000,000",
            ));
        }
        let salt = part(salt).ok_or_else(|| fail("the salt is not 16 to 64 bytes of base64"))?;
        let hash = part(hash).ok_or_else(|| fail("the hash is not 16 to 64 bytes of base64"))?;
        if length.is_some_and(|length| usize::try_from(length).ok() != Some(hash.len())) {
            return Err(fail("the length `l` is not the hash's"));
        }
        Ok(Self {
            iterations: NonZeroU32::new(iterations).ok_or_else(|| fail("no iterations"))?,
            salt,
            hash,
        })
    }

    /// Hashes `password` with a fresh random salt and [`DEFAULT_ITERATIONS`].
    ///
    /// # Errors
    ///
    /// [`Error::Random`] when the system's random source fails.
    pub fn new(password: &[u8]) -> Result<Self, Error> {
        Self::with_iterations(password, DEFAULT_ITERATIONS)
    }

    /// Hashes `password` with a fresh random salt and `iterations`, which is clamped to
    /// [`MIN_ITERATIONS`] to [`MAX_ITERATIONS`].
    ///
    /// # Errors
    ///
    /// [`Error::Random`] when the system's random source fails.
    pub fn with_iterations(password: &[u8], iterations: u32) -> Result<Self, Error> {
        let mut salt = [0u8; SALT_LEN];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Random)?;
        let iterations = iterations.clamp(MIN_ITERATIONS, MAX_ITERATIONS);
        Ok(Self::derive(password, iterations, &salt))
    }

    /// The hash of `password` with `salt`, for a count already in range.
    pub(crate) fn derive(password: &[u8], iterations: u32, salt: &[u8]) -> Self {
        let iterations = NonZeroU32::new(iterations).unwrap_or(NonZeroU32::MIN);
        let mut hash = [0u8; HASH_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            salt,
            password,
            &mut hash,
        );
        Self {
            iterations,
            salt: salt.into(),
            hash: hash.into(),
        }
    }

    /// The iteration count.
    pub fn iterations(&self) -> u32 {
        self.iterations.get()
    }

    /// Whether `password` is the password this hash was made from. The derived keys are
    /// compared in constant time, inside aws-lc.
    pub fn verify(&self, password: &[u8]) -> bool {
        pbkdf2::verify(
            pbkdf2::PBKDF2_HMAC_SHA256,
            self.iterations,
            &self.salt,
            password,
            &self.hash,
        )
        .is_ok()
    }
}

/// A non-empty run of ASCII digits, without a sign or leading zeros, as a `u32`.
fn decimal(text: &str) -> Option<u32> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical.then(|| text.parse().ok()).flatten()
}

/// A salt or a hash: base64 without padding, of a size this crate accepts.
fn part(text: &str) -> Option<Box<[u8]>> {
    let bytes = B64.decode(text).ok()?;
    (MIN_PART..=MAX_PART)
        .contains(&bytes.len())
        .then(|| bytes.into_boxed_slice())
}

impl fmt::Display for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "${ALGORITHM}$i={},l={}${}${}",
            self.iterations,
            self.hash.len(),
            B64.encode(&self.salt),
            B64.encode(&self.hash)
        )
    }
}

impl fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PasswordHash")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

impl FromStr for PasswordHash {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        Self::parse(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hash of `correct horse` with a salt of sixteen 0x01 bytes, written by an independent
    /// implementation (Python's hashlib.pbkdf2_hmac) when this test was written.
    const KNOWN: &str = "$pbkdf2-sha256$i=100000,l=32$AQEBAQEBAQEBAQEBAQEBAQ\
                         $xOI1F8Rk3lh4vYZ0kAW2IhEJiQ4YHQWTP/PQ1JLu/OU";

    #[test]
    fn a_hash_made_elsewhere_verifies() {
        let hash = PasswordHash::parse(KNOWN).unwrap();
        assert_eq!(hash.iterations(), 100_000);
        assert!(hash.verify(b"correct horse"));
        assert!(!hash.verify(b"correct horse "));
        assert!(!hash.verify(b""));
        assert_eq!(hash.to_string(), KNOWN);
    }

    #[test]
    fn a_new_hash_round_trips_through_its_string() {
        let hash = PasswordHash::with_iterations(b"s3cret", MIN_ITERATIONS).unwrap();
        let text = hash.to_string();
        assert!(text.starts_with("$pbkdf2-sha256$i=100000,l=32$"), "{text}");
        let read: PasswordHash = text.parse().unwrap();
        assert_eq!(read, hash);
        assert!(read.verify(b"s3cret"));
        assert!(!read.verify(b"s3cret!"));
        // A fresh salt every time.
        let again = PasswordHash::with_iterations(b"s3cret", MIN_ITERATIONS).unwrap();
        assert_ne!(again.to_string(), text);
        // The count is held to its bounds.
        let low = PasswordHash::with_iterations(b"x", 1).unwrap();
        assert_eq!(low.iterations(), MIN_ITERATIONS);
        assert_eq!(
            format!("{low:?}"),
            "PasswordHash { iterations: 100000, .. }"
        );
    }

    #[test]
    fn strings_that_are_not_ours_are_refused() {
        let salt = "AQEBAQEBAQEBAQEBAQEBAQ";
        let hash = "xOI1F8Rk3lh4vYZ0kAW2IhEJiQ4YHQWTP/PQ1JLu/OU";
        let cases = [
            "".to_owned(),
            "plain".to_owned(),
            format!("pbkdf2-sha256$i=100000${salt}${hash}"),
            format!("$argon2id$v=19$m=19456,t=2,p=1${salt}${hash}"),
            format!("$pbkdf2-sha512$i=100000${salt}${hash}"),
            format!("$pbkdf2-sha256$i=100000${salt}"),
            format!("$pbkdf2-sha256$i=100000${salt}${hash}$"),
            format!("$pbkdf2-sha256$100000${salt}${hash}"),
            format!("$pbkdf2-sha256$i=99999${salt}${hash}"),
            format!("$pbkdf2-sha256$i=10000001${salt}${hash}"),
            format!("$pbkdf2-sha256$i=0100000${salt}${hash}"),
            format!("$pbkdf2-sha256$i=+100000${salt}${hash}"),
            format!("$pbkdf2-sha256$i=100000,i=100000${salt}${hash}"),
            format!("$pbkdf2-sha256$i=100000,r=8${salt}${hash}"),
            format!("$pbkdf2-sha256$l=32${salt}${hash}"),
            format!("$pbkdf2-sha256$i=100000,l=31${salt}${hash}"),
            format!("$pbkdf2-sha256$i=100000$AQEBAQ${hash}"),
            format!("$pbkdf2-sha256$i=100000${salt}=${hash}"),
            format!("$pbkdf2-sha256$i=100000${salt}${hash}=="),
            format!("$pbkdf2-sha256$i=100000${salt}$!{hash}"),
        ];
        for text in cases {
            let error = PasswordHash::parse(&text).unwrap_err();
            assert!(matches!(error, Error::PasswordHash { .. }), "{text}");
            // The message never quotes what it refused.
            assert!(!error.to_string().contains(hash), "{error}");
        }
    }
}
