//! The password list (report R2, rule 5): the users a listener with `enable_authn` accepts,
//! loaded from a bootstrap file when the cluster starts.
//!
//! A bootstrap file holds one user a line, `name,secret`: the name, a comma, and everything
//! after the comma, exactly. In the `hashed` format the secret is a PHC string ([`PasswordHash`]);
//! in the `plain` format it is the password, hashed as the file is loaded so that no plain
//! password stays in memory. Blank lines and lines starting with `#` are skipped. A name is at
//! least one character, holds no comma and no U+0000, and neither starts nor ends with
//! whitespace.
//!
//! With a [`ReservedPrefix`], a name that begins with it is a service credential and any other
//! name an ordinary user, and the list refuses to give the prefix to anything but a service
//! (R2 rule 15).

mod authenticator;
mod hash;

use std::collections::HashMap;
use std::collections::hash_map;
use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

pub use self::authenticator::PasswordAuthenticator;
pub use self::hash::{ALGORITHM, DEFAULT_ITERATIONS, MAX_ITERATIONS, MIN_ITERATIONS, PasswordHash};
use crate::{Error, ReservedPrefix};

/// How a bootstrap file holds its passwords, as the setting `auth.password_bootstrap_type`
/// names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BootstrapFormat {
    /// `name,password`, each password hashed as the file is loaded.
    Plain,
    /// `name,hash`, each hash a PHC string.
    Hashed,
}

impl BootstrapFormat {
    /// The name the setting uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Hashed => "hashed",
        }
    }
}

impl fmt::Display for BootstrapFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BootstrapFormat {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        match text {
            "plain" => Ok(Self::Plain),
            "hashed" => Ok(Self::Hashed),
            _ => Err(Error::BootstrapFormat {
                name: text.to_owned(),
            }),
        }
    }
}

/// What a credential in the list is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CredentialClass {
    /// An ordinary user: a device or a person.
    User,
    /// A service, whose name carries the reserved prefix (R2 rule 15).
    Service,
}

/// What [`PasswordList::check`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Check {
    /// The password is the user's.
    Valid(CredentialClass),
    /// The user is in the list, and the password is not theirs.
    Invalid,
    /// The list has no such user.
    Unknown,
}

#[derive(Clone, Debug)]
struct Entry {
    hash: PasswordHash,
    class: CredentialClass,
}

/// Users and their password hashes.
#[derive(Clone, Debug, Default)]
pub struct PasswordList {
    entries: HashMap<Box<str>, Entry>,
    reserved: Option<ReservedPrefix>,
}

impl PasswordList {
    /// An empty list, keeping `reserved` for service credentials.
    pub fn new(reserved: Option<ReservedPrefix>) -> Self {
        Self {
            entries: HashMap::new(),
            reserved,
        }
    }

    /// Reads a bootstrap file, `text`, in `format`. A name with the `reserved` prefix is a
    /// service credential, any other an ordinary user.
    ///
    /// # Errors
    ///
    /// [`Error::Bootstrap`] for every line it cannot read, the first one only: a line that is
    /// not `name,secret`, a name it refuses or has seen before, or a hash that is not a
    /// pbkdf2-sha256 PHC string. [`Error::Random`] when no salt can be drawn for a plain
    /// password.
    pub fn parse(
        text: &str,
        format: BootstrapFormat,
        reserved: Option<ReservedPrefix>,
    ) -> Result<Self, Error> {
        let mut list = Self::new(reserved);
        let mut lines = HashMap::new();
        for (index, line) in text.split('\n').enumerate() {
            let number = index + 1;
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let fail = |reason: String| Error::Bootstrap {
                line: number,
                reason,
            };
            let (name, secret) = line.split_once(',').ok_or_else(|| {
                fail("a user is `name,secret`, and this line has no comma".into())
            })?;
            if name.trim() != name {
                return Err(fail(
                    "a user name neither starts nor ends with whitespace".into(),
                ));
            }
            check_name(name).map_err(|error| fail(error.to_string()))?;
            if let Some(first) = lines.insert(name.to_owned(), number) {
                return Err(fail(format!(
                    "the user `{name}` is already on line {first}"
                )));
            }
            let hash = match format {
                BootstrapFormat::Hashed => PasswordHash::parse(secret)
                    .map_err(|error| fail(format!("the hash of `{name}` is {error}")))?,
                BootstrapFormat::Plain if secret.is_empty() => {
                    return Err(fail(format!("the password of `{name}` is empty")));
                }
                BootstrapFormat::Plain => PasswordHash::new(secret.as_bytes())?,
            };
            let added = if list.reserves(name) {
                list.add_service(name, hash)
            } else {
                list.add_user(name, hash)
            };
            added.map_err(|error| fail(error.to_string()))?;
        }
        Ok(list)
    }

    /// The prefix kept for service credentials, if there is one.
    pub fn reserved(&self) -> Option<&ReservedPrefix> {
        self.reserved.as_ref()
    }

    fn reserves(&self, name: &str) -> bool {
        self.reserved
            .as_ref()
            .is_some_and(|prefix| prefix.reserves(name))
    }

    /// Adds or replaces an ordinary user.
    ///
    /// # Errors
    ///
    /// [`Error::UserName`] when the name is empty, longer than a User Name can be, holds
    /// U+0000, or begins with the reserved prefix, which only services carry (R2 rule 15).
    pub fn add_user(&mut self, name: &str, hash: PasswordHash) -> Result<(), Error> {
        check_name(name)?;
        if self.reserves(name) {
            return Err(Error::UserName {
                name: name.to_owned(),
                reason: "begins with the prefix reserved for service credentials",
            });
        }
        self.insert(name, hash, CredentialClass::User);
        Ok(())
    }

    /// Adds or replaces a service credential.
    ///
    /// # Errors
    ///
    /// [`Error::UserName`] when the name is not one [`add_user`](Self::add_user) would take
    /// but for the prefix, when no prefix is reserved, or when the name does not begin with it.
    pub fn add_service(&mut self, name: &str, hash: PasswordHash) -> Result<(), Error> {
        check_name(name)?;
        if !self.reserves(name) {
            return Err(Error::UserName {
                name: name.to_owned(),
                reason: "does not begin with a prefix reserved for service credentials",
            });
        }
        self.insert(name, hash, CredentialClass::Service);
        Ok(())
    }

    fn insert(&mut self, name: &str, hash: PasswordHash, class: CredentialClass) {
        self.entries.insert(name.into(), Entry { hash, class });
    }

    /// Removes a user; false if there was none.
    pub fn remove(&mut self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }

    /// The class of the user `name`, if the list has one.
    pub fn class(&self, name: &str) -> Option<CredentialClass> {
        self.entries.get(name).map(|entry| entry.class)
    }

    /// How many users there are.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The users' names, in no particular order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|name| &**name)
    }

    /// Checks `password` for `name`. It costs a hash whether or not the user exists, so the
    /// time it takes does not say which names are in the list.
    pub fn check(&self, name: &str, password: &[u8]) -> Check {
        match self.entries.get(name) {
            Some(entry) if entry.hash.verify(password) => Check::Valid(entry.class),
            Some(_) => Check::Invalid,
            None => {
                let _ = decoy().verify(password);
                Check::Unknown
            }
        }
    }
}

impl<'a> IntoIterator for &'a PasswordList {
    type Item = (&'a str, &'a PasswordHash);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        Iter(self.entries.iter())
    }
}

/// The users of a [`PasswordList`] and their hashes, in no particular order.
pub struct Iter<'a>(hash_map::Iter<'a, Box<str>, Entry>);

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a str, &'a PasswordHash);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(name, entry)| (&**name, &entry.hash))
    }
}

/// What an unknown user's password is checked against, at the default cost: a hash of
/// nothing, with a salt no list uses.
fn decoy() -> &'static PasswordHash {
    static DECOY: OnceLock<PasswordHash> = OnceLock::new();
    DECOY.get_or_init(|| PasswordHash::derive(b"", DEFAULT_ITERATIONS, b"openqtt-no-user!"))
}

/// The rules every name in the list follows.
fn check_name(name: &str) -> Result<(), Error> {
    let reason = if name.is_empty() {
        "is empty"
    } else if name.len() > 65_535 {
        "is longer than 65,535 bytes"
    } else if name.contains('\0') {
        "contains U+0000"
    } else {
        return Ok(());
    };
    Err(Error::UserName {
        name: name.chars().take(64).collect(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(password: &str) -> PasswordHash {
        PasswordHash::with_iterations(password.as_bytes(), MIN_ITERATIONS).unwrap()
    }

    fn hashed_file(users: &[(&str, &str)]) -> String {
        users
            .iter()
            .map(|(name, password)| format!("{name},{}\n", hash(password)))
            .collect()
    }

    #[test]
    fn r2_rule_5_a_hashed_file_checks_passwords() {
        let text = format!(
            "# services\n{}\n\r\n{}",
            hashed_file(&[("svc:platform", "p1")]),
            hashed_file(&[("alice", "p2")]).replace('\n', "\r\n")
        );
        let prefix = ReservedPrefix::new("svc:").unwrap();
        let list = PasswordList::parse(&text, BootstrapFormat::Hashed, Some(prefix)).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.check("svc:platform", b"p1"),
            Check::Valid(CredentialClass::Service)
        );
        assert_eq!(
            list.check("alice", b"p2"),
            Check::Valid(CredentialClass::User)
        );
        assert_eq!(list.check("alice", b"p1"), Check::Invalid);
        assert_eq!(list.check("alice", b""), Check::Invalid);
        assert_eq!(list.check("bob", b"p2"), Check::Unknown);
        let mut names: Vec<_> = list.names().collect();
        names.sort_unstable();
        assert_eq!(names, ["alice", "svc:platform"]);
        assert_eq!((&list).into_iter().count(), 2);
    }

    #[test]
    fn a_plain_password_is_everything_after_the_first_comma() {
        let list = PasswordList::parse("alice,a, b,c\n", BootstrapFormat::Plain, None).unwrap();
        assert_eq!(
            list.check("alice", b"a, b,c"),
            Check::Valid(CredentialClass::User)
        );
        assert_eq!(list.check("alice", b"a"), Check::Invalid);
        // What is kept is a hash.
        let (_, hash) = (&list).into_iter().next().unwrap();
        assert_eq!(hash.iterations(), DEFAULT_ITERATIONS);
    }

    #[test]
    fn lines_that_cannot_be_read_name_their_line_and_never_their_secret() {
        let good = hash("p");
        let cases = [
            ("alice\n", 1, "has no comma"),
            (
                "# fine\nalice,x\n",
                2,
                "the hash of `alice` is not a pbkdf2-sha256",
            ),
            (",x\n", 1, "is empty"),
            (" alice,x\n", 1, "whitespace"),
            ("alice ,x\n", 1, "whitespace"),
            ("a\0,x\n", 1, "U+0000"),
        ];
        for (text, line, reason) in cases {
            let error = PasswordList::parse(text, BootstrapFormat::Hashed, None).unwrap_err();
            let Error::Bootstrap { line: at, .. } = &error else {
                panic!("{error:?}");
            };
            assert_eq!(*at, line, "{text:?}");
            assert!(error.to_string().contains(reason), "{error}");
        }
        let error = PasswordList::parse("alice,\n", BootstrapFormat::Plain, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "line 1: the password of `alice` is empty"
        );
        let twice = format!("alice,{good}\nbob,{good}\nalice,{good}\n");
        let error = PasswordList::parse(&twice, BootstrapFormat::Hashed, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "line 3: the user `alice` is already on line 1"
        );
        let secret = "$pbkdf2-sha256$i=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAA";
        let error = PasswordList::parse(&format!("alice,{secret}"), BootstrapFormat::Hashed, None)
            .unwrap_err();
        assert!(!error.to_string().contains(secret), "{error}");
    }

    #[test]
    fn r2_rule_15_the_list_gives_the_reserved_prefix_to_services_only() {
        let prefix = ReservedPrefix::new("svc:").unwrap();
        let mut list = PasswordList::new(Some(prefix.clone()));
        assert_eq!(list.reserved(), Some(&prefix));
        let error = list.add_user("svc:platform", hash("p")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the user name `svc:platform` begins with the prefix reserved for service credentials"
        );
        list.add_service("svc:platform", hash("p")).unwrap();
        assert!(list.add_service("platform", hash("p")).is_err());
        list.add_user("platform", hash("p")).unwrap();
        assert_eq!(list.class("svc:platform"), Some(CredentialClass::Service));
        assert_eq!(list.class("platform"), Some(CredentialClass::User));
        // Without a prefix there are no services.
        let mut open = PasswordList::new(None);
        assert!(open.add_service("svc:platform", hash("p")).is_err());
        open.add_user("svc:platform", hash("p")).unwrap();
        assert!(open.remove("svc:platform"));
        assert!(!open.remove("svc:platform"));
        assert!(open.is_empty());
    }

    #[test]
    fn the_format_reads_as_the_setting_names_it() {
        assert_eq!(
            "plain".parse::<BootstrapFormat>().unwrap(),
            BootstrapFormat::Plain
        );
        assert_eq!(
            "hashed".parse::<BootstrapFormat>().unwrap(),
            BootstrapFormat::Hashed
        );
        assert!("hash".parse::<BootstrapFormat>().is_err());
        assert_eq!(BootstrapFormat::Hashed.to_string(), "hashed");
    }
}
