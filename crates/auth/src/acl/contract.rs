//! Rules held against the device contract of report R2, rules 13 to 16: what the 1.x converter
//! warns about, and refuses with `--strict`.
//!
//! The check is static and errs towards reporting: a rule is a conflict unless it plainly
//! cannot let a device do what the contract forbids. A device is any client whose user name is
//! not a service credential's: an exact name the deployment lists, or one with its reserved
//! prefix. An allow rule is excused only by an earlier deny that applies to every client, at
//! every QoS, and covers what the allow rule would grant.

use regex_syntax::hir::literal::Extractor;

use super::spec::{Decision, NameMatch, QosSet, RuleSpec, TopicSpec, Who};
use super::topic::covers;

/// The deployment rules are checked against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Contract {
    /// The filters devices receive commands on, as their rules see them, before the
    /// mountpoint (R2 rules 7 and 13). `commands/#` by default.
    pub commands: Vec<String>,
    /// The user name prefix reserved for service credentials (R2 rule 15), if there is one.
    pub service_prefix: Option<String>,
    /// Service credentials named exactly.
    pub services: Vec<String>,
}

impl Default for Contract {
    fn default() -> Self {
        Self {
            commands: vec!["commands/#".to_owned()],
            service_prefix: None,
            services: Vec::new(),
        }
    }
}

/// A rule that conflicts with the contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    /// The rule, counted from 0.
    pub rule: usize,
    /// The rule of R2 it conflicts with, 13 to 16.
    pub r2_rule: u8,
    /// What it would allow.
    pub message: String,
}

/// Every conflict of `rules` with `contract`, by rule.
pub fn check(rules: &[RuleSpec], contract: &Contract) -> Vec<Conflict> {
    let mut conflicts = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        if rule.decision != Decision::Allow {
            continue;
        }
        let mut conflict = |r2_rule, message: String| {
            conflicts.push(Conflict {
                rule: index,
                r2_rule,
                message,
            });
        };
        let earlier = &rules[..index];
        let device = may_be_device(&rule.who, contract);
        if device && rule.action.publishes() {
            for command in &contract.commands {
                let reaches = rule.topics.iter().any(|topic| reaches(topic, command));
                if reaches
                    && !denied_to_everyone(earlier, false, |topic| topic_covers(topic, command))
                {
                    conflict(
                        13,
                        format!("a device may publish to the command topics {command}"),
                    );
                }
            }
            let retained = rule.retain != Some(false);
            let excused = rule.topics.iter().all(|topic| {
                denied_to_everyone(earlier, true, |deny| {
                    matches!(deny, TopicSpec::All) || allow_covered(deny, topic)
                })
            });
            if retained && !excused {
                conflict(16, "a device may publish with RETAIN set".to_owned());
            }
        }
        if rule.who.address.is_some() && !rule.who.names_identity() {
            conflict(
                14,
                "the client's address alone grants the rule's rights".to_owned(),
            );
        }
        if rule.who.client_id.is_some()
            && rule.who.username.is_none()
            && rule.who.attributes.is_empty()
        {
            conflict(
                15,
                "the rule grants by client identifier, which a client chooses for itself"
                    .to_owned(),
            );
        }
        if let (Some(prefix), Some(names)) = (&contract.service_prefix, &rule.who.username) {
            for name in names {
                match name {
                    NameMatch::Prefix(shorter)
                        if prefix.starts_with(shorter.as_str()) && shorter != prefix =>
                    {
                        conflict(
                            15,
                            format!(
                                "the prefix {shorter:?} matches the reserved {prefix:?} and names \
                                 outside it, so any of them can claim a service's rights"
                            ),
                        );
                    }
                    NameMatch::Regex(pattern) if may_match_reserved(pattern, prefix) => conflict(
                        15,
                        format!(
                            "the pattern {pattern:?} may match names with the reserved prefix \
                             {prefix:?}; a rule for services names them exactly or by the \
                             reserved prefix"
                        ),
                    ),
                    _ => {}
                }
            }
        }
    }
    conflicts
}

/// Whether a rule for `who` may apply to a device: it does unless it names users, and each name
/// it accepts is a service's.
fn may_be_device(who: &Who, contract: &Contract) -> bool {
    let Some(names) = &who.username else {
        return true;
    };
    let reserved = |name: &str| {
        contract
            .service_prefix
            .as_ref()
            .is_some_and(|prefix| name.starts_with(prefix.as_str()))
    };
    !names.iter().all(|name| match name {
        NameMatch::Exact(name) => contract.services.contains(name) || reserved(name),
        NameMatch::Prefix(prefix) => reserved(prefix),
        NameMatch::Regex(_) => false,
    })
}

/// Whether some earlier deny, for every client and at every QoS, covering publishes (with
/// RETAIN set when `retained`), has a topic `covering` accepts.
fn denied_to_everyone(
    earlier: &[RuleSpec],
    retained: bool,
    covering: impl Fn(&TopicSpec) -> bool,
) -> bool {
    earlier.iter().any(|deny| {
        deny.decision == Decision::Deny
            && deny.who.is_everyone()
            && deny.action.publishes()
            && deny.qos == QosSet::ALL
            && (deny.retain.is_none() || (retained && deny.retain == Some(true)))
            && deny.topics.iter().any(&covering)
    })
}

/// Whether a publish matched by `topic` may land on a topic of the filter `command`.
fn reaches(topic: &TopicSpec, command: &str) -> bool {
    match topic {
        TopicSpec::All => true,
        // A publish names a topic, never a wildcard, so only a name can be equal to this.
        TopicSpec::Exact(text) => !text.contains(['+', '#']) && intersects(text, command),
        TopicSpec::Filter(filter) => intersects(filter, command),
    }
}

/// Whether the deny topic `deny` takes in every topic of the command filter `command`.
fn topic_covers(deny: &TopicSpec, command: &str) -> bool {
    match deny {
        TopicSpec::All => true,
        TopicSpec::Filter(filter) => !filter.contains("${") && covers(filter, command),
        TopicSpec::Exact(_) => false,
    }
}

/// Whether the deny topic `deny` takes in every topic the allow topic `allow` grants.
fn allow_covered(deny: &TopicSpec, allow: &TopicSpec) -> bool {
    match (deny, allow) {
        (TopicSpec::All, _) => true,
        (TopicSpec::Filter(deny), TopicSpec::Filter(allow)) => {
            if deny.contains("${") {
                false
            } else if allow.contains("${") {
                // A client's value may stand for any levels, but never begins a `$`-topic.
                let dollar = allow.starts_with('$') && !allow.starts_with("${");
                deny == "#" && !dollar
            } else {
                covers(deny, allow)
            }
        }
        (TopicSpec::Filter(deny), TopicSpec::Exact(text)) => {
            !deny.contains("${") && !text.contains(['+', '#']) && covers(deny, text)
        }
        _ => false,
    }
}

/// Whether some topic matches both filters. A level with a placeholder may become any number
/// of levels, so it is taken as `#`.
fn intersects(a: &str, b: &str) -> bool {
    let wildcard_first = |text: &str| text.starts_with(['+', '#']) || text.starts_with("${");
    if (a.starts_with('$') && !a.starts_with("${") && wildcard_first(b))
        || (b.starts_with('$') && !b.starts_with("${") && wildcard_first(a))
    {
        return false;
    }
    let mut a = a.split('/');
    let mut b = b.split('/');
    loop {
        match (a.next(), b.next()) {
            (None, None) => return true,
            (Some(x), _) if x == "#" || x.contains("${") => return true,
            (_, Some(y)) if y == "#" || y.contains("${") => return true,
            (Some(x), Some(y)) if x == "+" || y == "+" || x == y => {}
            _ => return false,
        }
    }
}

/// Whether some name `pattern`, matched whole, takes in may begin with `prefix`: yes, unless the
/// pattern provably keeps away from it.
///
/// The proof is the set of literals every match must begin with, as regex-syntax extracts them
/// for prefilters. When that set is every name the pattern matches (all its literals exact),
/// the pattern keeps away from the prefix if none of them begins with it. Otherwise a literal is
/// only how a match begins, and each must part from the prefix before either ends. A set too
/// wide to list, as for `.*` or `[a-z]`, proves nothing, and neither does a pattern that does
/// not parse.
fn may_match_reserved(pattern: &str, prefix: &str) -> bool {
    let Ok(hir) = regex_syntax::Parser::new().parse(&format!("^(?:{pattern})$")) else {
        return true;
    };
    let starts = Extractor::new().extract(&hir);
    let Some(literals) = starts.literals() else {
        return true;
    };
    let whole = starts.is_exact();
    let prefix = prefix.as_bytes();
    literals.iter().any(|literal| {
        let start = literal.as_bytes();
        start.starts_with(prefix) || (!whole && prefix.starts_with(start))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::parse_rules;

    fn conflicts(text: &str, contract: &Contract) -> Vec<(usize, u8)> {
        let rules = parse_rules(text).unwrap();
        check(&rules, contract)
            .into_iter()
            .map(|conflict| (conflict.rule, conflict.r2_rule))
            .collect()
    }

    fn services() -> Contract {
        Contract {
            service_prefix: Some("svc:".into()),
            services: vec!["platform".into()],
            ..Contract::default()
        }
    }

    #[test]
    fn r2_rule_13_publishing_commands_is_a_conflict_unless_denied_first() {
        let open = r##"
[[rule]]
permission = "allow"
action = "all"
retain = false
topics = ["#"]
"##;
        assert_eq!(conflicts(open, &Contract::default()), [(0, 13)]);
        let denied = r##"
[[rule]]
permission = "deny"
action = "publish"
topics = ["commands/#"]

[[rule]]
permission = "allow"
action = "all"
retain = false
topics = ["#"]
"##;
        assert_eq!(conflicts(denied, &Contract::default()), []);
        // A deny at one QoS only excuses nothing.
        let partial = denied.replace(
            "topics = [\"commands/#\"]",
            "qos = 1\ntopics = [\"commands/#\"]",
        );
        assert_eq!(conflicts(&partial, &Contract::default()), [(1, 13)]);
        // A rule for services only is not a device's.
        let service = r##"
[[rule]]
permission = "allow"
username = ["platform", { prefix = "svc:" }]
action = "all"
topics = ["#"]
"##;
        assert_eq!(conflicts(service, &services()), []);
        assert_eq!(conflicts(service, &Contract::default()), [(0, 13), (0, 16)]);
        // Topics that cannot reach commands are fine; a client's own name might.
        let own = r##"
[[rule]]
permission = "allow"
action = "publish"
retain = false
topics = ["telemetry/#", { eq = "commands/#" }]

[[rule]]
permission = "allow"
action = "publish"
retain = false
topics = ["${username}/x"]
"##;
        assert_eq!(conflicts(own, &Contract::default()), [(1, 13)]);
    }

    #[test]
    fn r2_rule_14_an_address_alone_is_a_conflict() {
        let text = r##"
[[rule]]
permission = "deny"
address = "127.0.0.1"
action = "all"
topics = ["#"]
"##;
        assert_eq!(conflicts(text, &Contract::default()), []);
        let mut rules = parse_rules(text).unwrap();
        rules[0].decision = Decision::Allow;
        rules[0].action = super::super::spec::ActionKind::Subscribe;
        let found: Vec<_> = check(&rules, &Contract::default())
            .into_iter()
            .map(|conflict| conflict.r2_rule)
            .collect();
        assert_eq!(found, [14]);
    }

    #[test]
    fn r2_rule_15_naming_cannot_claim_a_service() {
        let by_id = r##"
[[rule]]
permission = "allow"
client_id = "svc:platform"
action = "subscribe"
topics = ["#"]
"##;
        assert_eq!(conflicts(by_id, &services()), [(0, 15)]);
        let shorter = r##"
[[rule]]
permission = "allow"
username = { prefix = "svc" }
action = "subscribe"
topics = ["#"]
"##;
        assert_eq!(conflicts(shorter, &services()), [(0, 15)]);
        let pattern = r##"
[[rule]]
permission = "allow"
username = { regex = "svc.*" }
action = "subscribe"
topics = ["#"]
"##;
        assert_eq!(conflicts(pattern, &services()), [(0, 15)]);
        // Whatever names a pattern takes, unless each provably starts away from the prefix.
        for overlapping in [
            "(svc:admin|device)",
            "(?i)SVC:root",
            ".*admin",
            "[a-z:]+",
            "s.*",
        ] {
            let text = pattern.replace("svc.*", overlapping);
            assert_eq!(conflicts(&text, &services()), [(0, 15)], "{overlapping}");
        }
        for apart in ["dev-[0-9]+", "(device|sensor)-[0-9]+", "svc", "sv", ""] {
            let text = pattern.replace("svc.*", apart);
            assert_eq!(conflicts(&text, &services()), [], "{apart:?}");
        }
        let exact = r##"
[[rule]]
permission = "allow"
username = ["svc:platform", { prefix = "svc:" }, { regex = "dev-[0-9]+" }]
action = "subscribe"
topics = ["#"]
"##;
        assert_eq!(conflicts(exact, &services()), []);
    }

    #[test]
    fn r2_rule_16_setting_retain_is_a_conflict_unless_denied_first() {
        let text = r##"
[[rule]]
permission = "deny"
action = "publish"
topics = ["commands/#"]

[[rule]]
permission = "allow"
action = "publish"
topics = ["telemetry/#"]
"##;
        assert_eq!(conflicts(text, &Contract::default()), [(1, 16)]);
        let denied = r##"
[[rule]]
permission = "deny"
action = "publish"
retain = true
topics = [{ all = true }]

[[rule]]
permission = "allow"
action = "publish"
topics = ["telemetry/#"]
"##;
        assert_eq!(conflicts(denied, &Contract::default()), []);
        let narrow = text.replace(
            "topics = [\"telemetry/#\"]",
            "retain = false\ntopics = [\"telemetry/#\"]",
        );
        assert_eq!(conflicts(&narrow, &Contract::default()), []);
    }

    #[test]
    fn filters_intersect_where_a_topic_matches_both() {
        assert!(intersects("#", "commands/#"));
        assert!(intersects("+/x", "commands/#"));
        assert!(intersects("commands/fw", "commands/#"));
        assert!(!intersects("telemetry/#", "commands/#"));
        assert!(!intersects("$SYS/#", "+/x"));
        assert!(intersects("${username}/x", "commands/#"));
        assert!(!intersects("a/b", "a/b/c"));
        assert!(intersects("a/b", "a/b/#"));
    }
}
