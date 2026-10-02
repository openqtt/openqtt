//! Writing rules as TOML, for the 1.x converter: each rule a `[[rule]]` table that reads back
//! to the same rule.

use std::fmt::Write as _;

use super::spec::{NameMatch, QosSet, RuleSpec, TopicSpec};

/// `rules` as an ACL file, with nothing else in it.
pub fn to_toml(rules: &[RuleSpec]) -> String {
    let mut text = String::from("version = 1\n");
    for rule in rules {
        text.push('\n');
        write_rule(rule, &mut text);
    }
    text
}

/// Appends `rule` to `text` as a `[[rule]]` table.
pub fn write_rule(rule: &RuleSpec, text: &mut String) {
    text.push_str("[[rule]]\n");
    let _ = writeln!(text, "permission = {}", string(rule.decision.as_str()));
    if let Some(matchers) = &rule.who.username {
        let _ = writeln!(text, "username = {}", matchers_value(matchers));
    }
    if let Some(matchers) = &rule.who.client_id {
        let _ = writeln!(text, "client_id = {}", matchers_value(matchers));
    }
    if let Some(networks) = &rule.who.address {
        let networks: Vec<String> = networks
            .iter()
            .map(|network| string(&network.to_string()))
            .collect();
        let _ = writeln!(text, "address = {}", one_or_array(networks));
    }
    if !rule.who.attributes.is_empty() {
        let pairs: Vec<String> = rule
            .who
            .attributes
            .iter()
            .map(|(name, matchers)| format!("{} = {}", key(name), matchers_value(matchers)))
            .collect();
        let _ = writeln!(text, "attributes = {{ {} }}", pairs.join(", "));
    }
    let _ = writeln!(text, "action = {}", string(rule.action.as_str()));
    if rule.qos != QosSet::ALL {
        let levels: Vec<String> = rule.qos.iter().map(|qos| qos.value().to_string()).collect();
        let _ = writeln!(text, "qos = [{}]", levels.join(", "));
    }
    if let Some(retain) = rule.retain {
        let _ = writeln!(text, "retain = {retain}");
    }
    let topics: Vec<String> = rule
        .topics
        .iter()
        .map(|topic| match topic {
            TopicSpec::Filter(filter) => string(filter),
            TopicSpec::Exact(text) => format!("{{ eq = {} }}", string(text)),
            TopicSpec::All => "{ all = true }".to_owned(),
        })
        .collect();
    let _ = writeln!(text, "topics = [{}]", topics.join(", "));
}

fn matchers_value(matchers: &[NameMatch]) -> String {
    one_or_array(
        matchers
            .iter()
            .map(|matcher| match matcher {
                NameMatch::Exact(name) => string(name),
                NameMatch::Prefix(prefix) => format!("{{ prefix = {} }}", string(prefix)),
                NameMatch::Regex(pattern) => format!("{{ regex = {} }}", string(pattern)),
            })
            .collect(),
    )
}

fn one_or_array(mut values: Vec<String>) -> String {
    if values.len() == 1 {
        values.remove(0)
    } else {
        format!("[{}]", values.join(", "))
    }
}

/// A TOML key: bare when it can be, quoted otherwise.
fn key(name: &str) -> String {
    let bare = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if bare { name.to_owned() } else { basic(name) }
}

/// A TOML string: a literal string, which keeps backslashes as they are, for text that has
/// some and nothing a literal string cannot hold; a basic string otherwise.
pub(crate) fn string(text: &str) -> String {
    let literal_ok = !text.contains('\'') && !text.chars().any(|c| c.is_control() && c != '\t');
    if text.contains('\\') && literal_ok {
        format!("'{text}'")
    } else {
        basic(text)
    }
}

fn basic(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for c in text.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(quoted, "\\u{:04X}", u32::from(c));
            }
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::parse::parse;
    use crate::acl::spec::{ActionKind, Cidr, Decision, Who};

    #[test]
    fn strings_read_back_as_written() {
        for text in [
            "plain",
            "with \"quotes\"",
            r"back\slash",
            "both \\ and '",
            "line\nbreak\ttab\u{7f}\u{1}",
            "\u{1f600} caf\u{e9}",
            "",
        ] {
            let toml_text = format!("value = {}\n", string(text));
            let table: toml::Table = toml::from_str(&toml_text).unwrap();
            assert_eq!(table["value"].as_str(), Some(text), "{toml_text}");
        }
        assert_eq!(string(r"dev-\d+"), r"'dev-\d+'");
        assert_eq!(key("org"), "org");
        assert_eq!(key("x.y"), "\"x.y\"");
    }

    #[test]
    fn rules_read_back_as_written() {
        let mut first = RuleSpec::new(
            Decision::Allow,
            ActionKind::Publish,
            vec![
                TopicSpec::Filter("${username}/t/#".into()),
                TopicSpec::Exact("#".into()),
                TopicSpec::All,
            ],
        );
        first.who = Who {
            username: Some(vec![
                NameMatch::Exact("svc:platform".into()),
                NameMatch::Regex(r"dev-\d+".into()),
            ]),
            client_id: Some(vec![NameMatch::Prefix("c-".into())]),
            address: Some(vec![Cidr::parse("10.0.0.0/8").unwrap()]),
            attributes: vec![("org".into(), vec![NameMatch::Exact("acme".into())])],
        };
        first.qos = QosSet::EMPTY.with(openqtt_core::QoS::AtLeastOnce);
        first.retain = Some(true);
        let second = RuleSpec::new(Decision::Deny, ActionKind::All, vec![TopicSpec::All]);
        let rules = vec![first, second];
        let text = to_toml(&rules);
        let (read, _) = parse(&text).unwrap();
        assert_eq!(read, rules, "{text}");
    }
}
