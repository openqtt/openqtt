//! Every trait works as a trait object, and an implementation that writes only what it must
//! gets the defaults: what a build of the broker relies on when it hands the node its
//! extensions as `Arc<dyn ...>`.

use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use openqtt_core::{ClientId, Message, QoS, SubOpts, Timestamp, TopicFilter, TopicName, Username};
use openqtt_ext::{
    Ack, Action, AuthExchange, Authenticator, Authorizer, BoxFuture, Challenge, ClientInfo,
    ConnectInfo, Connected, DisconnectReason, Disconnected, Entry, Error, ExternalId, Forwarder,
    Grant, Interest, InterestRegistry, InterestSource, LogConsumer, Permission, Principal,
    Redirect, RedirectCause, RedirectKind, RedirectPolicy, Refusal, Secret, ServerReference,
    SessionEvents, Subscribed, TakenOver, Unsubscribed, Verdict,
};

/// Polls a future that never waits to completion.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the future was not ready"),
    }
}

fn client(name: &str) -> ClientInfo {
    ClientInfo::new(
        ClientId::new(name).expect("a valid client id"),
        Principal::new(Some(Username::new(name).expect("a valid user name"))),
    )
}

/// A password check, and a challenge for one enhanced method.
struct Passwords;

impl Authenticator for Passwords {
    fn handles(&self, method: Option<&str>) -> bool {
        matches!(method, None | Some("TEST-CHALLENGE"))
    }

    fn authenticate<'a>(&'a self, connect: &'a ConnectInfo) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            if connect.authentication_method.is_some() {
                return Verdict::Continue(Challenge::new(
                    Some(Secret::new(&b"nonce"[..])),
                    Box::new(Exchange),
                ));
            }
            match (&connect.username, &connect.password) {
                (Some(user), Some(password)) if password.expose() == b"right" => {
                    Verdict::Allow(Grant::new(Principal::new(Some(user.clone()))))
                }
                (Some(_), _) => Verdict::Deny(Refusal::BadUserNameOrPassword),
                (None, _) => Verdict::Pass,
            }
        })
    }
}

struct Exchange;

impl AuthExchange for Exchange {
    fn step(self: Box<Self>, data: Option<Secret>) -> BoxFuture<'static, Verdict> {
        Box::pin(async move {
            match data {
                Some(answer) if answer.expose() == b"nonce-signed" => {
                    Verdict::Allow(Grant::new(Principal::new(None)))
                }
                _ => Verdict::Deny(Refusal::NotAuthorized),
            }
        })
    }
}

#[test]
fn an_authenticator_is_a_trait_object() {
    let authenticator: Arc<dyn Authenticator> = Arc::new(Passwords);
    assert!(authenticator.handles(None));
    assert!(!authenticator.handles(Some("SCRAM-SHA-256")));
    let connect = ConnectInfo::new("192.0.2.1:1000".parse().unwrap(), "default")
        .with_username(Username::new("svc").unwrap())
        .with_password(Secret::new(&b"right"[..]));
    let Verdict::Allow(grant) = ready(authenticator.authenticate(&connect)) else {
        panic!("the right password was refused");
    };
    assert_eq!(grant.principal.username.unwrap().as_str(), "svc");
    let wrong = connect.clone().with_password(Secret::new(&b"wrong"[..]));
    assert!(matches!(
        ready(authenticator.authenticate(&wrong)),
        Verdict::Deny(Refusal::BadUserNameOrPassword)
    ));
    let anonymous = ConnectInfo::new("192.0.2.1:1000".parse().unwrap(), "default");
    assert!(matches!(
        ready(authenticator.authenticate(&anonymous)),
        Verdict::Pass
    ));
}

#[test]
fn enhanced_authentication_hands_the_exchange_on() {
    let authenticator: Arc<dyn Authenticator> = Arc::new(Passwords);
    let connect = ConnectInfo::new("192.0.2.1:1000".parse().unwrap(), "default")
        .with_authentication("TEST-CHALLENGE", None);
    let Verdict::Continue(challenge) = ready(authenticator.authenticate(&connect)) else {
        panic!("no challenge");
    };
    assert_eq!(challenge.data.as_ref().unwrap().expose(), b"nonce");
    let answer = Secret::new(&b"nonce-signed"[..]);
    assert!(matches!(
        ready(challenge.exchange.step(Some(answer))),
        Verdict::Allow(_)
    ));
}

/// Publishes only under the client's own name; everything else is denied.
struct OwnTopics;

impl Authorizer for OwnTopics {
    fn authorize(&self, client: &ClientInfo, action: &Action<'_>) -> Permission {
        let own = client.client_id.as_str();
        match action {
            Action::Publish { topic, .. } if topic.as_str().starts_with(own) => Permission::Allow,
            _ => Permission::Deny,
        }
    }
}

#[test]
fn an_authorizer_binds_to_a_client_by_default() {
    let authorizer: Arc<dyn Authorizer> = Arc::new(OwnTopics);
    let bound = authorizer.bind(client("pump-3"));
    assert_eq!(bound.client().client_id.as_str(), "pump-3");
    let own = TopicName::new("pump-3/temperature").unwrap();
    let other = TopicName::new("pump-4/temperature").unwrap();
    let filter = TopicFilter::new("pump-3/#").unwrap();
    assert_eq!(
        bound.authorize(&Action::publish(&own, QoS::AtLeastOnce, false)),
        Permission::Allow
    );
    assert!(
        bound
            .authorize(&Action::publish(&own, QoS::AtMostOnce, true))
            .is_allowed()
    );
    assert_eq!(
        bound.authorize(&Action::publish(&other, QoS::AtLeastOnce, false)),
        Permission::Deny
    );
    assert_eq!(
        bound.authorize(&Action::subscribe(&filter, QoS::AtMostOnce)),
        Permission::Deny
    );
    assert_eq!(
        bound.authorize(&Action::receive(&own, QoS::AtMostOnce, false)),
        Permission::Deny
    );
}

/// Counts what it hears, and writes only two of the five methods.
#[derive(Default)]
struct Counter(Mutex<Vec<String>>);

impl SessionEvents for Counter {
    fn connected(&self, event: &Connected) {
        let line = format!("connected {}", event.client.client_id);
        self.0.lock().expect("not poisoned").push(line);
    }

    fn disconnected(&self, event: &Disconnected) {
        let line = format!("disconnected {} {:?}", event.client_id, event.reason);
        self.0.lock().expect("not poisoned").push(line);
    }
}

#[test]
fn session_events_default_to_nothing() {
    let counter = Arc::new(Counter::default());
    let events: Arc<dyn SessionEvents> = counter.clone();
    let id = ClientId::new("pump-3").unwrap();
    let at = Timestamp::from_unix_nanos(1);
    let filter = TopicFilter::new("commands/#").unwrap();
    let mounted = TopicFilter::new("ingest/pump-3/commands/#").unwrap();
    events.connected(&Connected::new(client("pump-3"), false, at));
    events.subscribed(&Subscribed::new(
        id.clone(),
        filter.clone(),
        mounted.clone(),
        SubOpts::new(QoS::AtLeastOnce),
        at,
    ));
    events.unsubscribed(&Unsubscribed::new(id.clone(), filter, mounted, at));
    events.taken_over(&TakenOver::new(id.clone(), at));
    events.disconnected(&Disconnected::new(id, DisconnectReason::server(0x8E), at));
    assert_eq!(
        *counter.0.lock().unwrap(),
        [
            "connected pump-3",
            "disconnected pump-3 Server { code: 142 }"
        ]
    );
}

/// Registers one filter and forwards to a list.
#[derive(Default)]
struct Bridge {
    sent: Mutex<Vec<(ExternalId, String)>>,
}

impl InterestSource for Bridge {
    fn start(&self, registry: Arc<dyn InterestRegistry>) {
        let interest = Interest::new(
            TopicFilter::new("ingest/+/telemetry/#").expect("a valid filter"),
            ExternalId::new(7),
            true,
        );
        registry.register(interest.clone()).expect("registered");
        registry.withdraw(&interest).expect("withdrawn");
    }
}

impl Forwarder for Bridge {
    fn forward(&self, destination: ExternalId, message: &Message) -> Ack {
        self.sent
            .lock()
            .expect("not poisoned")
            .push((destination, message.topic.to_string()));
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct Registry(Mutex<Vec<String>>);

impl InterestRegistry for Registry {
    fn register(&self, interest: Interest) -> Result<(), Error> {
        let line = format!("+{} {}", interest.destination, interest.filter);
        self.0.lock().expect("not poisoned").push(line);
        Ok(())
    }

    fn withdraw(&self, interest: &Interest) -> Result<(), Error> {
        let line = format!("-{} {}", interest.destination, interest.filter);
        self.0.lock().expect("not poisoned").push(line);
        Ok(())
    }
}

#[test]
fn a_bridge_registers_interest_and_forwards() {
    let bridge = Arc::new(Bridge::default());
    let source: Arc<dyn InterestSource> = bridge.clone();
    let forwarder: Arc<dyn Forwarder> = bridge.clone();
    let registry = Arc::new(Registry::default());
    source.start(registry.clone());
    assert_eq!(
        *registry.0.lock().unwrap(),
        ["+7 ingest/+/telemetry/#", "-7 ingest/+/telemetry/#"]
    );
    let message = Message::new(
        TopicName::new("ingest/pump-3/telemetry/t").unwrap(),
        Bytes::from_static(b"21.5"),
    );
    let ack = forwarder.forward(ExternalId::new(7), &message);
    drop(message);
    assert_eq!(ready(ack), Ok(()));
    assert_eq!(bridge.sent.lock().unwrap().len(), 1);
}

/// Keeps the sequences it was given.
#[derive(Default)]
struct Archive(Mutex<Vec<(u32, u64)>>);

impl LogConsumer for Archive {
    fn name(&self) -> &str {
        "archive"
    }

    fn consume<'a>(&'a self, batch: &'a [Entry]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut seen = self.0.lock().expect("not poisoned");
            seen.extend(batch.iter().map(|entry| (entry.partition, entry.sequence)));
            Ok(())
        })
    }
}

#[test]
fn a_log_consumer_reads_every_partition_by_default() {
    let archive = Arc::new(Archive::default());
    let consumer: Arc<dyn LogConsumer> = archive.clone();
    assert_eq!(consumer.name(), "archive");
    assert!(consumer.reads(0) && consumer.reads(255));
    let message = Message::new(TopicName::new("t").unwrap(), Bytes::new());
    let batch = [
        Entry::new(3, 10, message.clone()),
        Entry::new(3, 11, message),
    ];
    assert_eq!(ready(consumer.consume(&batch)), Ok(()));
    assert_eq!(*archive.0.lock().unwrap(), [(3, 10), (3, 11)]);
}

/// Sends drained clients to a fixed neighbour and moves nobody.
struct Neighbour;

impl RedirectPolicy for Neighbour {
    fn server_reference(&self, redirect: &Redirect) -> Option<ServerReference> {
        (redirect.cause == RedirectCause::Drain)
            .then(|| ServerReference::new("edge-2.example.net").expect("a valid reference"))
    }
}

#[test]
fn a_redirect_policy_is_a_trait_object() {
    let policy: Arc<dyn RedirectPolicy> = Arc::new(Neighbour);
    let drain = Redirect::new(
        client("pump-3"),
        RedirectKind::UseAnotherServer,
        RedirectCause::Drain,
    );
    assert_eq!(
        policy.server_reference(&drain).unwrap().as_str(),
        "edge-2.example.net"
    );
    let fenced = Redirect::new(
        client("pump-3"),
        RedirectKind::UseAnotherServer,
        RedirectCause::Fenced,
    );
    assert_eq!(policy.server_reference(&fenced), None);
}
