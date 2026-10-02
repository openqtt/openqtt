//! The 1.x converters against kept results: a synthetic `acl.conf` in the style of a 1.x
//! deployment (`tests/convert/acl.conf`, no real names in it) must convert to exactly
//! `tests/convert/acl.toml`, with exactly the notes of `tests/convert/acl.notes`, and the
//! result must decide as the file says (R2 rule 30). After an intended change, rewrite the kept
//! files and read the diff before committing it:
//!
//! ```text
//! cargo test -p openqtt-auth --test convert -- --ignored bless
//! ```

use std::sync::Arc;

use openqtt_auth::Acl;
use openqtt_auth::acl::contract::Contract;
use openqtt_auth::convert::{AclConversion, convert_acl, convert_authn};
use openqtt_auth::password::{BootstrapFormat, Check, CredentialClass, PasswordList};
use openqtt_core::{ClientId, QoS, TopicFilter, TopicName, Username};
use openqtt_ext::{Action, ClientInfo, Permission, Principal};

const SOURCE: &str = include_str!("convert/acl.conf");
const KEPT_TOML: &str = include_str!("convert/acl.toml");
const KEPT_NOTES: &str = include_str!("convert/acl.notes");

/// The deployment of the synthetic file: commands under `commands/`, services named `svc:`.
fn contract() -> Contract {
    Contract {
        service_prefix: Some("svc:".to_owned()),
        ..Contract::default()
    }
}

fn convert() -> AclConversion {
    convert_acl(SOURCE, &contract()).expect("the synthetic file converts")
}

/// The notes as `openqtt convert acl` prints them, without the file's name.
fn notes(converted: &AclConversion) -> String {
    let mut text = String::new();
    for note in &converted.warnings {
        text.push_str(&format!("line {}: warning: {}\n", note.line, note.message));
    }
    for note in &converted.conflicts {
        text.push_str(&format!("line {}: conflict: {}\n", note.line, note.message));
    }
    text
}

#[test]
fn the_synthetic_acl_converts_as_kept() {
    let converted = convert();
    assert_eq!(converted.text, KEPT_TOML, "the converted file changed");
    assert_eq!(notes(&converted), KEPT_NOTES, "the notes changed");
}

#[test]
#[ignore = "rewrites the kept results"]
fn bless() {
    let converted = convert();
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/convert");
    std::fs::write(format!("{dir}/acl.toml"), &converted.text).expect("written");
    std::fs::write(format!("{dir}/acl.notes"), notes(&converted)).expect("written");
}

fn client(username: Option<&str>, client_id: &str, address: &str) -> ClientInfo {
    ClientInfo::new(
        ClientId::new(client_id).expect("an identifier"),
        Principal::new(username.map(|name| Username::new(name).expect("a name"))),
    )
    .with_address(address.parse().expect("an address"))
}

fn publish(acl: &Arc<Acl>, client: &ClientInfo, topic: &str, qos: u8, retain: bool) -> Permission {
    let topic = TopicName::new(topic).expect("a topic");
    acl.bind(client).decide(&Action::publish(
        &topic,
        QoS::from_u8(qos).expect("a QoS"),
        retain,
    ))
}

fn subscribe(acl: &Arc<Acl>, client: &ClientInfo, filter: &str) -> Permission {
    let filter = TopicFilter::new(filter).expect("a filter");
    acl.bind(client)
        .decide(&Action::subscribe(&filter, QoS::AtLeastOnce))
}

#[test]
fn r2_rule_30_the_converted_file_decides_as_the_1x_file() {
    let acl = Arc::new(Acl::from_toml(&convert().text).expect("the converted file loads"));
    let device = client(
        Some("acme/production/pump-3"),
        "acme/production/pump-3",
        "203.0.113.9:1",
    );
    use Permission::{Allow, Deny};
    // Devices: telemetry at QoS 0 and 1, never retained, never commands.
    assert_eq!(publish(&acl, &device, "telemetry/t", 1, false), Allow);
    assert_eq!(publish(&acl, &device, "telemetry/t", 2, false), Deny);
    assert_eq!(publish(&acl, &device, "telemetry/t", 1, true), Deny);
    assert_eq!(publish(&acl, &device, "events/boot/state", 0, false), Allow);
    assert_eq!(publish(&acl, &device, "commands/firmware", 1, false), Deny);
    assert_eq!(subscribe(&acl, &device, "commands/#"), Allow);
    assert_eq!(subscribe(&acl, &device, "config/${clientid}"), Allow);
    assert_eq!(subscribe(&acl, &device, "config/acme"), Deny);
    // Services.
    let platform = client(Some("svc:platform"), "p1", "10.0.0.5:1");
    assert_eq!(
        publish(&acl, &platform, "ingest/x/commands/fw", 1, true),
        Allow
    );
    let bridge = client(Some("svc:bridge"), "b1", "10.0.0.6:1");
    assert_eq!(
        publish(&acl, &bridge, "ingest/x/commands/fw", 1, false),
        Allow
    );
    // Dashboards, by name or by identifier, and the subscription to `#` exactly.
    let dashboard = client(Some("dashboard"), "d", "203.0.113.9:1");
    assert_eq!(subscribe(&acl, &dashboard, "ingest/#"), Allow);
    assert_eq!(subscribe(&acl, &dashboard, "#"), Allow);
    assert_eq!(subscribe(&acl, &dashboard, "+/#"), Deny);
    let by_id = client(Some("someone"), "dash-7", "203.0.113.9:1");
    assert_eq!(subscribe(&acl, &by_id, "ingest/acme/#"), Allow);
    // ops may do anything, `$`-topics included.
    let ops = client(Some("ops"), "o", "203.0.113.9:1");
    assert_eq!(publish(&acl, &ops, "$SYS/x", 2, true), Allow);
    // Legacy devices on their own topics, at QoS 1 without RETAIN.
    let legacy = client(Some("old"), "legacy-42", "203.0.113.9:1");
    assert_eq!(
        publish(&acl, &legacy, "legacy/legacy-42/x", 1, false),
        Allow
    );
    assert_eq!(publish(&acl, &legacy, "legacy/legacy-43/x", 1, false), Deny);
    assert_eq!(publish(&acl, &legacy, "legacy/legacy-42/x", 0, false), Deny);
    // The network the 1.x rule meant is denied everything.
    let banned = client(Some("ops"), "o", "192.0.2.200:1");
    assert_eq!(
        publish(&acl, &banned, "x", 0, false),
        Allow,
        "ops is allowed before the ban"
    );
    let stranger = client(Some("stranger"), "s", "192.0.2.200:1");
    assert_eq!(subscribe(&acl, &stranger, "commands/#"), Allow);
    assert_eq!(publish(&acl, &stranger, "telemetry/t", 0, false), Allow);
    // Changed: loopback is granted nothing by its address (R2 rule 14).
    let local = client(Some("monitor"), "m", "127.0.0.1:1");
    assert_eq!(subscribe(&acl, &local, "$SYS/brokers"), Deny);
}

#[test]
fn r2_rule_30_strict_refuses_what_conflicts_with_rules_13_to_16() {
    let converted = convert();
    let mut rules: Vec<u8> = converted
        .conflicts
        .iter()
        .map(|note| {
            note.message
                .strip_prefix("R2 rule ")
                .and_then(|rest| rest.get(..2))
                .and_then(|number| number.parse().ok())
                .expect("each conflict names its R2 rule")
        })
        .collect();
    rules.sort_unstable();
    rules.dedup();
    // Every rule of 13 to 16 is met at least once in the synthetic file.
    assert_eq!(rules, [13, 14, 15, 16]);
    // A file written to the contract has none.
    let clean = "{allow, {username, \"svc:platform\"}, all, all}.\n\
                 {deny, all, publish, [\"commands/#\"]}.\n\
                 {deny, all, {publish, [{retain, true}]}, all}.\n\
                 {allow, all, publish, [\"telemetry/#\"]}.\n\
                 {allow, all, subscribe, [\"commands/#\"]}.\n\
                 {deny, all}.\n";
    let services = Contract {
        services: vec!["svc:platform".to_owned()],
        ..contract()
    };
    let converted = convert_acl(clean, &services).expect("converts");
    assert_eq!(converted.conflicts, []);
    assert_eq!(converted.warnings, []);
}

#[test]
fn r2_rule_15_a_pattern_that_can_name_a_service_conflicts() {
    let source = "{allow, {username, {re, \"^(svc:admin|device)$\"}}, subscribe, [\"#\"]}.\n";
    let converted = convert_acl(source, &contract()).expect("converts");
    let r15: Vec<&str> = converted
        .conflicts
        .iter()
        .filter(|note| note.message.starts_with("R2 rule 15:"))
        .map(|note| note.message.as_str())
        .collect();
    assert_eq!(r15.len(), 1, "{:?}", converted.conflicts);
    // A pattern held to a literal start that is not the reserved prefix is no conflict.
    let devices = "{allow, {username, {re, \"^dev-[0-9]+$\"}}, subscribe, [\"#\"]}.\n";
    let converted = convert_acl(devices, &contract()).expect("converts");
    assert_eq!(converted.conflicts, []);
}

#[test]
fn r2_rule_14_an_address_branch_left_out_of_an_or_is_a_conflict() {
    let source = "{allow, {'or', [{ipaddr, \"127.0.0.1\"}, {username, \"monitor\"}]}, \
                  subscribe, [\"$SYS/#\"]}.\n";
    let converted = convert_acl(source, &Contract::default()).expect("converts");
    // The branch that names a client is kept; the one on an address alone is left out, and
    // --strict refuses the file for it.
    assert_eq!(converted.rules.len(), 1);
    let r14: Vec<_> = converted
        .conflicts
        .iter()
        .filter(|note| note.message.starts_with("R2 rule 14:"))
        .collect();
    assert_eq!(r14.len(), 1, "{:?}", converted.conflicts);
    assert_eq!(r14[0].line, 1);
    assert!(
        converted.text.contains("# Part not converted (R2 rule 14)"),
        "{}",
        converted.text
    );
}

#[test]
fn r2_rule_30_what_1x_would_not_load_is_refused() {
    for (source, reason) in [
        (
            "{allow, all, publish}.",
            "a rule is {Permission, Who, Action, Topics}",
        ),
        ("{maybe, all}.", "the permission is `allow` or `deny`"),
        (
            "{allow, {group, \"x\"}, all, all}.",
            "the clients are `all`",
        ),
        (
            "{allow, all, write, all}.",
            "the action is publish, subscribe or all",
        ),
        (
            "{allow, all, {publish, [{qos, 3}]}, all}.",
            "a QoS is 0, 1 or 2",
        ),
        ("{allow, all, all, \"t/#\"}.", "the topics are a list"),
        (
            "{allow, all, all, [\"${cert_common_name}/#\"]}.",
            "write ${username}",
        ),
        (
            "{allow, all, all, [\"${client_attrs.group}/#\"]}.",
            "a topic may use ${username} and ${clientid}",
        ),
        (
            "{allow, {username, {re, \"(?<=x)y\"}}, all, all}.",
            "is not one 2.0 can read",
        ),
        (
            "{allow, {'and', [{username, {re, \"a\"}}, {username, \"b\"}]}, all, all}.",
            "two conditions on the user name",
        ),
        ("{allow, all, all, all}", "a term ends with a full stop"),
    ] {
        let error = convert_acl(source, &Contract::default()).expect_err(source);
        assert!(error.to_string().contains(reason), "{source}: {error}");
    }
}

#[test]
fn r2_rule_5_a_1x_user_file_becomes_a_hashed_bootstrap_file() {
    let converted = convert_authn(
        "user_id,password,is_superuser\nsvc:platform,Synthetic-1,false\npump-3,Synthetic-2,false\n",
    )
    .expect("converts");
    assert_eq!(converted.users, 2);
    let list = PasswordList::parse(
        &converted.text,
        BootstrapFormat::Hashed,
        Some(openqtt_auth::ReservedPrefix::new("svc:").expect("a prefix")),
    )
    .expect("the converted file loads");
    assert_eq!(
        list.check("svc:platform", b"Synthetic-1"),
        Check::Valid(CredentialClass::Service)
    );
    assert_eq!(
        list.check("pump-3", b"Synthetic-2"),
        Check::Valid(CredentialClass::User)
    );
    let refused = convert_authn("user_id,password,is_superuser\nroot,Synthetic-3,true\n")
        .expect_err("a superuser is refused");
    assert!(refused.to_string().contains("superusers"), "{refused}");
}
