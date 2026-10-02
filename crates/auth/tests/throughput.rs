//! Decisions per second over a file of 100 rules shaped like a deployment's: services by exact
//! name and prefix, device classes by prefix and pattern, rules on the client's own topics, and
//! the device rules last, where every one before them has to be passed over.
//!
//! Ignored by default, since it means something only in release mode:
//!
//! ```text
//! cargo test --release -p openqtt-auth --test throughput -- --ignored --nocapture
//! ```
//!
//! It asserts nothing about time, which is the machine's; it reports, and checks the decisions.

use std::fmt::Write as _;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openqtt_auth::Acl;
use openqtt_core::{ClientId, QoS, TopicFilter, TopicName, Username};
use openqtt_ext::{Action, ClientInfo, Permission, Principal};

/// Decisions timed for each case.
const ROUNDS: u32 = 1_000_000;

/// The five device rules that end both files.
const DEVICE_RULES: [&str; 5] = [
    "permission = \"deny\"\naction = \"publish\"\ntopics = [\"commands/#\"]\n",
    "permission = \"deny\"\naction = \"publish\"\nretain = true\ntopics = [{ all = true }]\n",
    "permission = \"allow\"\naction = \"publish\"\ntopics = [\"telemetry/#\", \"events/#\"]\n",
    "permission = \"allow\"\naction = \"subscribe\"\ntopics = [\"commands/#\"]\n",
    "permission = \"deny\"\naction = \"all\"\ntopics = [{ all = true }]\n",
];

/// A file of exactly 100 rules, most of them for other principals.
fn rules() -> String {
    let mut text = String::from("version = 1\n");
    let mut rule = |body: &str| {
        let _ = write!(text, "\n[[rule]]\n{body}");
    };
    // 40 services, each by its exact name, from inside the cluster.
    for service in 0..40 {
        rule(&format!(
            "permission = \"allow\"\nusername = \"svc:service-{service}\"\naddress = \"10.0.0.0/8\"\naction = \"all\"\ntopics = [\"ingest/#\", \"$SYS/brokers/#\"]\n"
        ));
    }
    // 40 device classes, by prefix and by pattern, each with topics of its own.
    for class in 0..20 {
        rule(&format!(
            "permission = \"allow\"\nusername = {{ prefix = \"line-{class}/\" }}\naction = \"publish\"\nretain = false\ntopics = [\"telemetry/line-{class}/+/value\", \"${{username}}/state\"]\n"
        ));
        rule(&format!(
            "permission = \"allow\"\nusername = {{ regex = \"meter-{class}-[0-9]+\" }}\naction = \"all\"\nqos = [0, 1]\ntopics = [\"meters/{class}/${{clientid}}/#\"]\n"
        ));
    }
    // 15 operators' rules on attributes.
    for team in 0..15 {
        rule(&format!(
            "permission = \"allow\"\nattributes = {{ team = \"team-{team}\" }}\naction = \"subscribe\"\ntopics = [\"ingest/+/team-{team}/#\"]\n"
        ));
    }
    // The device rules, last.
    DEVICE_RULES.iter().for_each(|body| rule(body));
    text
}

/// A file of exactly 100 rules that all apply to every client, the worst case: each of the
/// first 95 has its topics compared, a template among them, before the device rules decide.
fn everyone_rules() -> String {
    let mut text = String::from("version = 1\n");
    for other in 0..95 {
        let _ = write!(
            text,
            "\n[[rule]]\npermission = \"allow\"\naction = \"all\"\ntopics = [\"other/{other}/#\", \"${{username}}/other/{other}\"]\n"
        );
    }
    for body in DEVICE_RULES {
        let _ = write!(text, "\n[[rule]]\n{body}");
    }
    text
}

struct Case {
    name: &'static str,
    expected: Permission,
}

fn per_second(count: u32, elapsed: Duration) -> f64 {
    f64::from(count) / elapsed.as_secs_f64()
}

#[test]
#[ignore = "a measurement, to run in release mode"]
#[expect(clippy::print_stderr, reason = "the test reports its measurements")]
fn decisions_per_second_on_a_100_rule_file() {
    let mut report = String::new();
    for (title, text) in [
        ("rules for many principals", rules()),
        ("every rule for every client", everyone_rules()),
    ] {
        measure(title, &text, &mut report);
    }
    eprint!("{report}");
}

fn measure(title: &str, text: &str, report: &mut String) {
    let acl = Arc::new(Acl::from_toml(text).expect("the file compiles"));
    assert_eq!(acl.len(), 100);
    let device = ClientInfo::new(
        ClientId::new("acme/production/pump-3").expect("an identifier"),
        Principal::new(Some(
            Username::new("acme/production/pump-3").expect("a name"),
        )),
    )
    .with_address("203.0.113.5:40000".parse().expect("an address"));

    let started = Instant::now();
    for _ in 0..10_000 {
        black_box(acl.bind(black_box(&device)));
    }
    let bind = started.elapsed() / 10_000;
    let bound = acl.bind(&device);

    let telemetry = TopicName::new("telemetry/boiler/temperature").expect("a topic");
    let command = TopicName::new("commands/firmware").expect("a topic");
    let stray = TopicName::new("somewhere/else").expect("a topic");
    let commands = TopicFilter::new("commands/#").expect("a filter");
    let cases = [
        (
            Case {
                name: "publish allowed by rule 98",
                expected: Permission::Allow,
            },
            Action::publish(&telemetry, QoS::AtLeastOnce, false),
        ),
        (
            Case {
                name: "publish denied by rule 96",
                expected: Permission::Deny,
            },
            Action::publish(&command, QoS::AtLeastOnce, false),
        ),
        (
            Case {
                name: "subscribe allowed by rule 99",
                expected: Permission::Allow,
            },
            Action::subscribe(&commands, QoS::AtLeastOnce),
        ),
        (
            Case {
                name: "publish denied by rule 100",
                expected: Permission::Deny,
            },
            Action::publish(&stray, QoS::AtMostOnce, false),
        ),
    ];
    let _ = writeln!(
        report,
        "{title}: {} rules, {} of them apply to the client, which binds in {bind:?}",
        acl.len(),
        bound.applicable()
    );
    for (case, action) in &cases {
        assert_eq!(bound.decide(action), case.expected, "{}", case.name);
        assert_eq!(acl.decide(&device, action), case.expected, "{}", case.name);
        let started = Instant::now();
        for _ in 0..ROUNDS {
            black_box(bound.decide(black_box(action)));
        }
        let bound_rate = per_second(ROUNDS, started.elapsed());
        let started = Instant::now();
        for _ in 0..ROUNDS / 10 {
            black_box(acl.decide(black_box(&device), black_box(action)));
        }
        let unbound_rate = per_second(ROUNDS / 10, started.elapsed());
        let _ = writeln!(
            report,
            "  {:<30} bound {:>12.0}/s   unbound {:>12.0}/s",
            case.name, bound_rate, unbound_rate
        );
    }
}
