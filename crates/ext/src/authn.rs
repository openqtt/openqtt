//! Authentication: deciding who a connecting client is.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use openqtt_core::{ClientId, Username};

use crate::{BoxFuture, Principal};

/// A secret a client sent: a password, or the data of an enhanced authentication step.
///
/// Its `Debug` shows only its length, and it has no `Display`, so a secret does not reach a log
/// by accident. It has no `PartialEq` either: comparing a secret is the authenticator's job,
/// and it compares in constant time. A clone shares the bytes.
#[derive(Clone)]
#[non_exhaustive]
pub struct Secret(Bytes);

impl Secret {
    /// The secret `bytes`.
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    /// The bytes, for the authenticator that checks them.
    pub fn expose(&self) -> &[u8] {
        &self.0
    }

    /// Its length in bytes, which is not secret.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secret(<redacted, {} bytes>)", self.0.len())
    }
}

/// A certificate, DER encoded, as a client presented it in the TLS handshake.
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Certificate(Bytes);

impl Certificate {
    /// The certificate encoded as `der`.
    pub fn from_der(der: impl Into<Bytes>) -> Self {
        Self(der.into())
    }

    /// Its DER encoding.
    pub fn der(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Certificate(<{} bytes of DER>)", self.0.len())
    }
}

/// What a client sent in its CONNECT and where it came from: what an [`Authenticator`] decides
/// on.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConnectInfo {
    /// The Client Identifier the CONNECT carried, or `None` when it was empty and the server
    /// is to assign one ([MQTT-3.1.3-6]).
    pub client_id: Option<ClientId>,
    /// The User Name, if the CONNECT had one.
    pub username: Option<Username>,
    /// The Password, if the CONNECT had one.
    pub password: Option<Secret>,
    /// The Authentication Method of enhanced authentication, if the CONNECT named one
    /// (section 4.12).
    pub authentication_method: Option<Arc<str>>,
    /// The Authentication Data that came with it: the CONNECT's, or on re-authentication the
    /// AUTH's.
    pub authentication_data: Option<Secret>,
    /// Whether this is a re-authentication of a connection already accepted (section
    /// 4.12.1), started by the client's AUTH with reason code 0x19.
    pub reauthenticating: bool,
    /// The address the client connects from, its own and not a load balancer's (R2 rule 27).
    pub address: SocketAddr,
    /// The certificate chain the client presented, leaf first, once the TLS handshake has
    /// verified it against the listener's client CAs. Empty when it presented none.
    pub certificates: Arc<[Certificate]>,
    /// The name of the listener, as configured.
    pub listener: Arc<str>,
}

impl ConnectInfo {
    /// A CONNECT from `address` on `listener`, with nothing else: set the fields the CONNECT
    /// carried with the `with_` methods.
    pub fn new(address: SocketAddr, listener: &str) -> Self {
        Self {
            client_id: None,
            username: None,
            password: None,
            authentication_method: None,
            authentication_data: None,
            reauthenticating: false,
            address,
            certificates: Arc::from(Vec::new()),
            listener: listener.into(),
        }
    }

    /// With the Client Identifier `client_id`.
    #[must_use]
    pub fn with_client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// With the User Name `username`.
    #[must_use]
    pub fn with_username(mut self, username: Username) -> Self {
        self.username = Some(username);
        self
    }

    /// With the Password `password`.
    #[must_use]
    pub fn with_password(mut self, password: Secret) -> Self {
        self.password = Some(password);
        self
    }

    /// With enhanced authentication by `method`, and the data that came with it.
    #[must_use]
    pub fn with_authentication(mut self, method: &str, data: Option<Secret>) -> Self {
        self.authentication_method = Some(method.into());
        self.authentication_data = data;
        self
    }

    /// As a re-authentication (section 4.12.1).
    #[must_use]
    pub fn as_reauthentication(mut self) -> Self {
        self.reauthenticating = true;
        self
    }

    /// With the verified certificate chain `certificates`, leaf first.
    #[must_use]
    pub fn with_certificates(mut self, certificates: Vec<Certificate>) -> Self {
        self.certificates = certificates.into();
        self
    }
}

/// Why a CONNECT is refused: the CONNACK reason code, 0x80 or above, after which the server
/// closes the connection ([MQTT-3.1.4-2], [MQTT-3.2.2-7]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Refusal {
    /// 0x80 Unspecified error: the server does not say why.
    UnspecifiedError,
    /// 0x86 Bad User Name or Password: a wrong or missing password (R2 rule 5).
    BadUserNameOrPassword,
    /// 0x87 Not authorized: the client may not connect, such as a certificate that does not
    /// carry the clientAuth extended key usage (R2 rule 3).
    NotAuthorized,
    /// 0x88 Server unavailable: what the authenticator relies on is down.
    ServerUnavailable,
    /// 0x89 Server busy: try again later.
    ServerBusy,
    /// 0x8A Banned by administrative action.
    Banned,
    /// 0x8C Bad authentication method: not supported, or not the one this connection was
    /// authenticated with (MQTT-4.12.0-1, MQTT-4.12.1-1).
    BadAuthenticationMethod,
}

impl Refusal {
    /// The reason code.
    pub const fn code(self) -> u8 {
        match self {
            Self::UnspecifiedError => 0x80,
            Self::BadUserNameOrPassword => 0x86,
            Self::NotAuthorized => 0x87,
            Self::ServerUnavailable => 0x88,
            Self::ServerBusy => 0x89,
            Self::Banned => 0x8A,
            Self::BadAuthenticationMethod => 0x8C,
        }
    }
}

/// An accepted CONNECT: who the client is.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Grant {
    /// Who the client is, for authorization and for the `${username}` of a mountpoint.
    pub principal: Principal,
    /// The Client Identifier its session is kept under, when the authenticator decides it: on
    /// a listener configured for certificate identity, the certificate's CN (R2 rule 4, R1
    /// D20). The server returns it as Assigned Client Identifier. `None` keeps the CONNECT's,
    /// or assigns one if that was empty.
    pub client_id: Option<ClientId>,
    /// Authentication Data for the CONNACK that ends an enhanced authentication.
    pub authentication_data: Option<Secret>,
}

impl Grant {
    /// The client is `principal`.
    pub fn new(principal: Principal) -> Self {
        Self {
            principal,
            client_id: None,
            authentication_data: None,
        }
    }

    /// And its session is kept under `client_id`.
    #[must_use]
    pub fn with_client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// And the CONNACK carries `data` as Authentication Data.
    #[must_use]
    pub fn with_authentication_data(mut self, data: Secret) -> Self {
        self.authentication_data = Some(data);
        self
    }
}

/// One more step of enhanced authentication: the server sends AUTH with reason code 0x18 and
/// `data`, and passes the client's answer to `exchange` ([MQTT-4.12.0-2]).
#[non_exhaustive]
pub struct Challenge {
    /// The Authentication Data of the server's AUTH.
    pub data: Option<Secret>,
    /// What decides on the client's answer.
    pub exchange: Box<dyn AuthExchange>,
}

impl Challenge {
    /// Send `data`, and give the answer to `exchange`.
    pub fn new(data: Option<Secret>, exchange: Box<dyn AuthExchange>) -> Self {
        Self { data, exchange }
    }
}

impl fmt::Debug for Challenge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Challenge")
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

/// What an authenticator decides about a CONNECT, or about one step of enhanced
/// authentication.
#[derive(Debug)]
#[non_exhaustive]
pub enum Verdict {
    /// The client may connect, as the grant says.
    Allow(Grant),
    /// The client may not: CONNACK with this reason code, then close.
    Deny(Refusal),
    /// Enhanced authentication takes another step (section 4.12).
    Continue(Challenge),
    /// This authenticator has no opinion: the next one decides. When every authenticator
    /// passes, the CONNECT is refused.
    Pass,
}

/// The state of one enhanced authentication between two steps, held by the connection.
pub trait AuthExchange: Send + 'static {
    /// Decides on the client's AUTH, whose Authentication Data is `data`. It is given the
    /// exchange by value, so that a [`Verdict::Continue`] can hand it on, changed or not.
    fn step(self: Box<Self>, data: Option<Secret>) -> BoxFuture<'static, Verdict>;
}

/// Decides who a connecting client is.
///
/// The edge asks its authenticators in order for each CONNECT, and the first that does not
/// [pass](Verdict::Pass) decides. It is asynchronous because an authenticator may need a store
/// or a service; it runs once per connection, never per message.
pub trait Authenticator: Send + Sync + 'static {
    /// Whether this authenticator decides on CONNECTs with the Authentication Method `method`,
    /// or with none when `method` is `None`, as passwords and certificates have. A method no
    /// authenticator handles gets CONNACK 0x8C (MQTT-4.12.0-1). By default, only CONNECTs
    /// without a method.
    fn handles(&self, method: Option<&str>) -> bool {
        method.is_none()
    }

    /// Decides on `connect`.
    fn authenticate<'a>(&'a self, connect: &'a ConnectInfo) -> BoxFuture<'a, Verdict>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_never_shows_its_bytes() {
        let secret = Secret::new(&b"hunter2"[..]);
        assert_eq!(format!("{secret:?}"), "Secret(<redacted, 7 bytes>)");
        assert_eq!(secret.expose(), b"hunter2");
        assert_eq!(secret.len(), 7);
        assert!(!secret.is_empty());
        let connect = ConnectInfo::new("192.0.2.1:1883".parse().unwrap(), "default")
            .with_username(Username::new("device").unwrap())
            .with_password(secret);
        let text = format!("{connect:?}");
        assert!(!text.contains("hunter2"), "{text}");
        assert!(text.contains("device"), "{text}");
    }

    #[test]
    fn refusals_carry_connack_codes() {
        let codes = [
            (Refusal::UnspecifiedError, 0x80),
            (Refusal::BadUserNameOrPassword, 0x86),
            (Refusal::NotAuthorized, 0x87),
            (Refusal::ServerUnavailable, 0x88),
            (Refusal::ServerBusy, 0x89),
            (Refusal::Banned, 0x8A),
            (Refusal::BadAuthenticationMethod, 0x8C),
        ];
        for (refusal, code) in codes {
            assert_eq!(refusal.code(), code, "{refusal:?}");
        }
    }
}
