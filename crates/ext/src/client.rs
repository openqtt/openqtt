//! Who a client is once authenticated: what authorization, events and redirects are told.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use openqtt_core::{ClientId, Username};

/// Named values authentication attached to a principal, such as a certificate's organisation
/// or a token's claims: what authorization rules can match besides the name.
///
/// Each name appears once; inserting a name again replaces its value. They are kept sorted by
/// name, so two sets with the same pairs compare equal however they were built. A clone shares
/// the text.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Attributes(Vec<(Arc<str>, Arc<str>)>);

impl Attributes {
    /// No attributes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `name` to `value`, and returns the value it replaced.
    pub fn insert(&mut self, name: &str, value: &str) -> Option<Arc<str>> {
        match self.0.binary_search_by(|(key, _)| (**key).cmp(name)) {
            Ok(at) => Some(std::mem::replace(&mut self.0[at].1, value.into())),
            Err(at) => {
                self.0.insert(at, (name.into(), value.into()));
                None
            }
        }
    }

    /// The value of `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .binary_search_by(|(key, _)| (**key).cmp(name))
            .ok()
            .map(|at| &*self.0[at].1)
    }

    /// Every name and value, by name.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(name, value)| (&**name, &**value))
    }

    /// How many there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Attributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for Attributes {
    fn from_iter<I: IntoIterator<Item = (&'a str, &'a str)>>(pairs: I) -> Self {
        let mut attributes = Self::new();
        for (name, value) in pairs {
            attributes.insert(name, value);
        }
        attributes
    }
}

/// Who a client is, as authentication established it: the name authorization rules and the
/// `${username}` of a mountpoint refer to, and the attributes that came with it.
///
/// On a listener configured for certificate identity the name is the certificate's CN (report
/// R2, rule 4); with a password, the User Name the password was checked for. A client that
/// connected without a User Name on a listener that does not authenticate has none, and a rule
/// that names a user never matches it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Principal {
    /// The name, if the client has one.
    pub username: Option<Username>,
    /// What authentication attached to it.
    pub attributes: Attributes,
}

impl Principal {
    /// A principal named `username`, with no attributes.
    pub fn new(username: Option<Username>) -> Self {
        Self {
            username,
            attributes: Attributes::new(),
        }
    }

    /// The same principal, with `name` set to `value`.
    #[must_use]
    pub fn with_attribute(mut self, name: &str, value: &str) -> Self {
        self.attributes.insert(name, value);
        self
    }
}

/// A connected client, as authorization, session events and redirects see it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientInfo {
    /// The Client Identifier its session is kept under: the one it sent, the one it was
    /// assigned ([MQTT-3.1.3-6]), or its certificate's CN (R2 rule 4).
    pub client_id: ClientId,
    /// Who authentication said it is.
    pub principal: Principal,
    /// The address it connects from, the client's own and not a load balancer's (R2 rule 27).
    /// `None` for a caller that has none, such as a test.
    pub address: Option<SocketAddr>,
    /// The name of the listener it connected to, as configured.
    pub listener: Option<Arc<str>>,
}

impl ClientInfo {
    /// A client with no address and no listener: set them with [`with_address`] and
    /// [`with_listener`].
    ///
    /// [`with_address`]: Self::with_address
    /// [`with_listener`]: Self::with_listener
    pub fn new(client_id: ClientId, principal: Principal) -> Self {
        Self {
            client_id,
            principal,
            address: None,
            listener: None,
        }
    }

    /// The same client, connecting from `address`.
    #[must_use]
    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = Some(address);
        self
    }

    /// The same client, on the listener named `listener`.
    #[must_use]
    pub fn with_listener(mut self, listener: &str) -> Self {
        self.listener = Some(listener.into());
        self
    }

    /// The principal's name, if it has one.
    pub fn username(&self) -> Option<&Username> {
        self.principal.username.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_are_unique_and_sorted_by_name() {
        let mut attributes = Attributes::new();
        assert!(attributes.is_empty());
        assert_eq!(attributes.insert("org", "acme"), None);
        assert_eq!(attributes.insert("class", "device"), None);
        assert_eq!(attributes.insert("org", "example").as_deref(), Some("acme"));
        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes.get("org"), Some("example"));
        assert_eq!(attributes.get("missing"), None);
        let pairs: Vec<_> = attributes.iter().collect();
        assert_eq!(pairs, [("class", "device"), ("org", "example")]);
        let built: Attributes = [("org", "example"), ("class", "device")]
            .into_iter()
            .collect();
        assert_eq!(built, attributes);
        assert_eq!(
            format!("{attributes:?}"),
            r#"{"class": "device", "org": "example"}"#
        );
    }

    #[test]
    fn a_client_carries_its_principal() {
        let client = ClientInfo::new(
            ClientId::new("pump-3").unwrap(),
            Principal::new(Some(Username::new("pump-3").unwrap())).with_attribute("org", "acme"),
        )
        .with_address("192.0.2.7:4242".parse().unwrap())
        .with_listener("devices");
        assert_eq!(client.username().map(Username::as_str), Some("pump-3"));
        assert_eq!(client.principal.attributes.get("org"), Some("acme"));
        assert_eq!(client.listener.as_deref(), Some("devices"));
        assert_eq!(client.address.unwrap().port(), 4242);
    }
}
