//! The password list as an [`Authenticator`].

use std::num::NonZeroUsize;
use std::sync::{Arc, PoisonError, RwLock};

use openqtt_ext::{Authenticator, BoxFuture, ConnectInfo, Grant, Principal, Refusal, Verdict};

use super::{Check, PasswordList};
use crate::Error;
use crate::pool::Pool;

/// Authenticates CONNECTs with a User Name and Password against a [`PasswordList`] (R2 rule 5).
///
/// - The right password: allowed, as the user.
/// - A wrong or missing password for a user in the list: CONNACK 0x86.
/// - A user the list does not have: no opinion, so that the next authenticator decides; when
///   there is none, the edge refuses the CONNECT. The check costs as much as for a user in the
///   list, so its time does not tell which names are.
/// - No User Name: no opinion.
///
/// A check costs a PBKDF2 hash, tens of milliseconds of CPU, so it runs on threads of its own
/// rather than on the async executor that holds the connections. They take CONNECTs from a
/// bounded queue, and a CONNECT that finds it full is refused at once with 0x89 Server busy:
/// a flood of passwords costs the threads given and no more.
///
/// The list is replaced whole, without a restart, when users change (R2 rule 5).
pub struct PasswordAuthenticator {
    list: RwLock<Arc<PasswordList>>,
    pool: Pool,
}

impl PasswordAuthenticator {
    /// Checks passwords from `list` on `threads` threads, with room for `queue` CONNECTs
    /// waiting for one.
    ///
    /// # Errors
    ///
    /// [`Error::Threads`] when not one thread could start.
    pub fn new(list: PasswordList, threads: NonZeroUsize, queue: usize) -> Result<Self, Error> {
        Ok(Self {
            list: RwLock::new(Arc::new(list)),
            pool: Pool::new("openqtt-password", threads, queue)?,
        })
    }

    /// The list in use.
    pub fn list(&self) -> Arc<PasswordList> {
        Arc::clone(&self.list.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Uses `list` from the next CONNECT on. A check already queued finishes with the list it
    /// started with.
    pub fn replace(&self, list: PasswordList) {
        *self.list.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(list);
    }
}

impl std::fmt::Debug for PasswordAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordAuthenticator")
            .field("users", &self.list().len())
            .finish_non_exhaustive()
    }
}

impl Authenticator for PasswordAuthenticator {
    fn authenticate<'a>(&'a self, connect: &'a ConnectInfo) -> BoxFuture<'a, Verdict> {
        let Some(username) = connect.username.clone() else {
            return Box::pin(std::future::ready(Verdict::Pass));
        };
        let list = self.list();
        let Some(password) = connect.password.clone() else {
            // Nothing to check, so nothing to hide by checking.
            let verdict = if list.class(username.as_str()).is_some() {
                Verdict::Deny(Refusal::BadUserNameOrPassword)
            } else {
                Verdict::Pass
            };
            return Box::pin(std::future::ready(verdict));
        };
        let name = username.clone();
        let job = self
            .pool
            .run(move || list.check(name.as_str(), password.expose()));
        Box::pin(async move {
            let Ok(pending) = job else {
                return Verdict::Deny(Refusal::ServerBusy);
            };
            match pending.await {
                Some(Check::Valid(_)) => Verdict::Allow(Grant::new(Principal::new(Some(username)))),
                Some(Check::Invalid) => Verdict::Deny(Refusal::BadUserNameOrPassword),
                Some(Check::Unknown) => Verdict::Pass,
                // The check failed inside aws-lc, which it never should.
                None => Verdict::Deny(Refusal::UnspecifiedError),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use openqtt_core::Username;
    use openqtt_ext::Secret;

    use super::*;
    use crate::ReservedPrefix;
    use crate::password::{MIN_ITERATIONS, PasswordHash};
    use crate::pool::tests::block_on;

    fn list() -> PasswordList {
        let mut list = PasswordList::new(Some(ReservedPrefix::new("svc:").unwrap()));
        let hash = |password: &str| {
            PasswordHash::with_iterations(password.as_bytes(), MIN_ITERATIONS).unwrap()
        };
        list.add_user("alice", hash("wonderland")).unwrap();
        list.add_service("svc:platform", hash("s3rvice")).unwrap();
        list
    }

    fn connect(username: Option<&str>, password: Option<&str>) -> ConnectInfo {
        let mut connect = ConnectInfo::new("192.0.2.1:50000".parse().unwrap(), "default");
        if let Some(name) = username {
            connect = connect.with_username(Username::new(name).unwrap());
        }
        if let Some(password) = password {
            connect = connect.with_password(Secret::new(password.as_bytes().to_vec()));
        }
        connect
    }

    fn decide(authenticator: &PasswordAuthenticator, connect: &ConnectInfo) -> Verdict {
        block_on(authenticator.authenticate(connect))
    }

    #[test]
    fn r2_rule_5_a_wrong_or_missing_password_gets_0x86() {
        let authenticator =
            PasswordAuthenticator::new(list(), NonZeroUsize::new(2).unwrap(), 16).unwrap();
        let Verdict::Allow(grant) =
            decide(&authenticator, &connect(Some("alice"), Some("wonderland")))
        else {
            panic!("the right password was refused");
        };
        assert_eq!(grant.principal.username.unwrap().as_str(), "alice");
        assert_eq!(grant.client_id, None);
        let Verdict::Allow(service) = decide(
            &authenticator,
            &connect(Some("svc:platform"), Some("s3rvice")),
        ) else {
            panic!("the service was refused");
        };
        assert_eq!(service.principal.username.unwrap().as_str(), "svc:platform");
        for (user, password) in [
            ("alice", Some("nope")),
            ("alice", Some("")),
            ("alice", None),
        ] {
            assert!(
                matches!(
                    decide(&authenticator, &connect(Some(user), password)),
                    Verdict::Deny(Refusal::BadUserNameOrPassword)
                ),
                "{user} {password:?}"
            );
        }
        // A name the list does not have, or none at all, is for another authenticator.
        assert!(matches!(
            decide(&authenticator, &connect(Some("mallory"), Some("x"))),
            Verdict::Pass
        ));
        assert!(matches!(
            decide(&authenticator, &connect(Some("mallory"), None)),
            Verdict::Pass
        ));
        assert!(matches!(
            decide(&authenticator, &connect(None, Some("x"))),
            Verdict::Pass
        ));
    }

    #[test]
    fn r2_rule_5_users_are_rotated_without_a_restart() {
        let authenticator =
            PasswordAuthenticator::new(list(), NonZeroUsize::new(1).unwrap(), 4).unwrap();
        let alice = connect(Some("alice"), Some("wonderland"));
        assert!(matches!(decide(&authenticator, &alice), Verdict::Allow(_)));
        let mut rotated = PasswordList::new(None);
        rotated
            .add_user(
                "alice",
                PasswordHash::with_iterations(b"looking-glass", MIN_ITERATIONS).unwrap(),
            )
            .unwrap();
        authenticator.replace(rotated);
        assert!(matches!(
            decide(&authenticator, &alice),
            Verdict::Deny(Refusal::BadUserNameOrPassword)
        ));
        let rotated_alice = connect(Some("alice"), Some("looking-glass"));
        assert!(matches!(
            decide(&authenticator, &rotated_alice),
            Verdict::Allow(_)
        ));
        assert_eq!(authenticator.list().len(), 1);
        assert_eq!(
            format!("{authenticator:?}"),
            "PasswordAuthenticator { users: 1, .. }"
        );
    }
}
