//! Listeners that do not authenticate.

use openqtt_ext::{Authenticator, BoxFuture, ConnectInfo, Grant, Principal, Refusal, Verdict};

use crate::ReservedPrefix;

/// Accepts every CONNECT as the User Name it carries, without a credential: what a listener
/// with `enable_authn` off does, for development or where something else, such as a network
/// boundary, decides who may connect.
///
/// It refuses, with CONNACK 0x87, a User Name that begins with the prefix reserved for service
/// credentials: a client that only names itself cannot claim a service's rights (R2 rule 15).
#[derive(Clone, Debug, Default)]
pub struct Anonymous {
    reserved: Option<ReservedPrefix>,
}

impl Anonymous {
    /// Accepts every CONNECT but those naming a user with `reserved`.
    pub fn new(reserved: Option<ReservedPrefix>) -> Self {
        Self { reserved }
    }
}

impl Authenticator for Anonymous {
    fn authenticate<'a>(&'a self, connect: &'a ConnectInfo) -> BoxFuture<'a, Verdict> {
        let claims_service = connect.username.as_ref().is_some_and(|name| {
            self.reserved
                .as_ref()
                .is_some_and(|prefix| prefix.reserves(name.as_str()))
        });
        let verdict = if claims_service {
            Verdict::Deny(Refusal::NotAuthorized)
        } else {
            Verdict::Allow(Grant::new(Principal::new(connect.username.clone())))
        };
        Box::pin(std::future::ready(verdict))
    }
}

#[cfg(test)]
mod tests {
    use openqtt_core::Username;

    use super::*;
    use crate::pool::tests::block_on;

    #[test]
    fn r2_rule_15_a_client_cannot_name_itself_a_service() {
        let anonymous = Anonymous::new(Some(ReservedPrefix::new("svc:").unwrap()));
        let at = "192.0.2.1:50000".parse().unwrap();
        let named =
            |name: &str| ConnectInfo::new(at, "dev").with_username(Username::new(name).unwrap());
        assert!(matches!(
            block_on(anonymous.authenticate(&named("svc:platform"))),
            Verdict::Deny(Refusal::NotAuthorized)
        ));
        let Verdict::Allow(grant) = block_on(anonymous.authenticate(&named("bench-17"))) else {
            panic!("an ordinary name was refused");
        };
        assert_eq!(grant.principal.username.unwrap().as_str(), "bench-17");
        let Verdict::Allow(grant) = block_on(anonymous.authenticate(&ConnectInfo::new(at, "dev")))
        else {
            panic!("a client without a name was refused");
        };
        assert_eq!(grant.principal.username, None);
    }
}
