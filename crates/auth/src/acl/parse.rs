//! Reading an ACL file: TOML into [`RuleSpec`]s, every problem reported at once, by rule and
//! line, without quoting a line of the file.

use openqtt_core::QoS;

use super::spec::{
    ActionKind, Cidr, CidrError, Decision, NameMatch, QosSet, RuleSpec, TopicSpec, Who,
};
use crate::Error;

/// The format's only version.
pub const VERSION: i64 = 1;

const TOP_KEYS: [&str; 2] = ["version", "rule"];
const RULE_KEYS: [&str; 9] = [
    "permission",
    "username",
    "client_id",
    "address",
    "attributes",
    "action",
    "qos",
    "retain",
    "topics",
];

/// The rules of `text`, in order, and the line each `[[rule]]` header is on, when every rule
/// has one.
pub(crate) fn parse(text: &str) -> Result<(Vec<RuleSpec>, Vec<Option<usize>>), Error> {
    let table: toml::Table = toml::from_str(text).map_err(|error| syntax(text, &error))?;
    let mut problems = Vec::new();
    for key in table.keys() {
        if !TOP_KEYS.contains(&key.as_str()) {
            problems.push(unknown_key(key, &TOP_KEYS, "the file"));
        }
    }
    match table.get("version") {
        None => {}
        Some(toml::Value::Integer(VERSION)) => {}
        Some(_) => problems.push(format!("`version` is {VERSION}, the only version there is")),
    }
    let rules = match table.get("rule") {
        None => &Vec::new(),
        Some(toml::Value::Array(rules)) => rules,
        Some(_) => {
            problems.push("`rule` is an array of tables, written `[[rule]]`".to_owned());
            &Vec::new()
        }
    };
    let lines = header_lines(text, rules.len());
    let mut specs = Vec::with_capacity(rules.len());
    for (index, rule) in rules.iter().enumerate() {
        let name = rule_name(index, &lines);
        let mut rule_problems = Vec::new();
        match rule {
            toml::Value::Table(rule) => {
                if let Some(spec) = parse_rule(rule, &mut rule_problems) {
                    specs.push(spec);
                }
            }
            _ => rule_problems.push("a rule is a table".to_owned()),
        }
        problems.extend(
            rule_problems
                .into_iter()
                .map(|problem| format!("{name}: {problem}")),
        );
    }
    if !problems.is_empty() {
        return Err(Error::Acl { problems });
    }
    Ok((specs, lines))
}

/// How a problem names the rule at `index`.
pub(crate) fn rule_name(index: usize, lines: &[Option<usize>]) -> String {
    match lines.get(index).copied().flatten() {
        Some(line) => format!("rule {} (line {line})", index + 1),
        None => format!("rule {}", index + 1),
    }
}

/// The line of each `[[rule]]` header, if there are exactly `count` of them, which there are
/// unless the file wrote its rules some other way.
fn header_lines(text: &str, count: usize) -> Vec<Option<usize>> {
    let headers: Vec<usize> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            line.trim()
                .strip_prefix("[[")
                .and_then(|rest| rest.split_once("]]"))
                .is_some_and(|(name, after)| {
                    let after = after.trim();
                    name.trim() == "rule" && (after.is_empty() || after.starts_with('#'))
                })
        })
        .map(|(index, _)| index + 1)
        .collect();
    if headers.len() == count {
        headers.into_iter().map(Some).collect()
    } else {
        vec![None; count]
    }
}

/// A TOML syntax error by line and column, without the line itself: a secret pasted in the
/// wrong place would be on it.
fn syntax(text: &str, error: &toml::de::Error) -> Error {
    let reason = error
        .message()
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("not valid TOML")
        .trim()
        .to_owned();
    let (line, column) = match error.span() {
        Some(span) => {
            let before = &text[..span.start.min(text.len())];
            let line = before.matches('\n').count() + 1;
            let column = before.rfind('\n').map_or(before.chars().count(), |at| {
                before[at + 1..].chars().count()
            }) + 1;
            (line, column)
        }
        None => (0, 0),
    };
    Error::AclSyntax {
        line,
        column,
        reason,
    }
}

fn parse_rule(rule: &toml::Table, problems: &mut Vec<String>) -> Option<RuleSpec> {
    for key in rule.keys() {
        if !RULE_KEYS.contains(&key.as_str()) {
            problems.push(unknown_key(key, &RULE_KEYS, "a rule"));
        }
    }
    let decision = match rule.get("permission") {
        Some(toml::Value::String(word)) if word == "allow" => Some(Decision::Allow),
        Some(toml::Value::String(word)) if word == "deny" => Some(Decision::Deny),
        Some(_) => {
            problems.push("`permission` is \"allow\" or \"deny\"".to_owned());
            None
        }
        None => {
            problems.push("`permission` is missing: \"allow\" or \"deny\"".to_owned());
            None
        }
    };
    let action = match rule.get("action") {
        Some(toml::Value::String(word)) => match word.as_str() {
            "publish" => Some(ActionKind::Publish),
            "subscribe" => Some(ActionKind::Subscribe),
            "all" => Some(ActionKind::All),
            _ => {
                problems.push("`action` is \"publish\", \"subscribe\" or \"all\"".to_owned());
                None
            }
        },
        Some(_) => {
            problems.push("`action` is \"publish\", \"subscribe\" or \"all\"".to_owned());
            None
        }
        None => {
            problems.push("`action` is missing: \"publish\", \"subscribe\" or \"all\"".to_owned());
            None
        }
    };
    let qos = match rule.get("qos") {
        None => QosSet::ALL,
        Some(value) => parse_qos(value, problems),
    };
    let retain = match rule.get("retain") {
        None => None,
        Some(toml::Value::Boolean(retain)) => Some(*retain),
        Some(_) => {
            problems.push("`retain` is true or false".to_owned());
            None
        }
    };
    let topics = match rule.get("topics") {
        Some(toml::Value::Array(entries)) => entries
            .iter()
            .filter_map(|entry| parse_topic(entry, problems))
            .collect(),
        Some(_) => {
            problems.push("`topics` is an array of topics".to_owned());
            Vec::new()
        }
        None => {
            problems.push("`topics` is missing".to_owned());
            Vec::new()
        }
    };
    let who = Who {
        username: rule
            .get("username")
            .map(|value| parse_matchers("username", value, problems)),
        client_id: rule
            .get("client_id")
            .map(|value| parse_matchers("client_id", value, problems)),
        address: rule
            .get("address")
            .map(|value| parse_networks(value, problems)),
        attributes: match rule.get("attributes") {
            None => Vec::new(),
            Some(toml::Value::Table(attributes)) => attributes
                .iter()
                .map(|(name, value)| {
                    let key = format!("attributes.{name}");
                    (name.clone(), parse_matchers(&key, value, problems))
                })
                .collect(),
            Some(_) => {
                problems.push("`attributes` is a table of names and values".to_owned());
                Vec::new()
            }
        },
    };
    Some(RuleSpec {
        decision: decision?,
        who,
        action: action?,
        qos,
        retain,
        topics,
    })
}

fn parse_qos(value: &toml::Value, problems: &mut Vec<String>) -> QosSet {
    let levels: Vec<&toml::Value> = match value {
        toml::Value::Array(levels) => levels.iter().collect(),
        single => vec![single],
    };
    let mut set = QosSet::EMPTY;
    for level in levels {
        match level
            .as_integer()
            .and_then(|level| u8::try_from(level).ok())
            .and_then(QoS::from_u8)
        {
            Some(qos) => set = set.with(qos),
            None => problems.push("`qos` lists QoS levels: 0, 1 or 2".to_owned()),
        }
    }
    set
}

fn parse_topic(value: &toml::Value, problems: &mut Vec<String>) -> Option<TopicSpec> {
    match value {
        toml::Value::String(filter) => Some(TopicSpec::Filter(filter.clone())),
        toml::Value::Table(table) if table.len() == 1 => {
            match (table.get("eq"), table.get("all")) {
                (Some(toml::Value::String(text)), _) => Some(TopicSpec::Exact(text.clone())),
                (_, Some(toml::Value::Boolean(true))) => Some(TopicSpec::All),
                _ => {
                    problems.push(TOPIC_FORMS.to_owned());
                    None
                }
            }
        }
        _ => {
            problems.push(TOPIC_FORMS.to_owned());
            None
        }
    }
}

const TOPIC_FORMS: &str = "a topic is a filter string, `{ eq = \"...\" }` or `{ all = true }`";

fn parse_matchers(key: &str, value: &toml::Value, problems: &mut Vec<String>) -> Vec<NameMatch> {
    let entries: Vec<&toml::Value> = match value {
        toml::Value::Array(entries) => entries.iter().collect(),
        single => vec![single],
    };
    let mut matchers = Vec::with_capacity(entries.len());
    for entry in entries {
        let matcher = match entry {
            toml::Value::String(exact) => Some(NameMatch::Exact(exact.clone())),
            toml::Value::Table(table) if table.len() == 1 => {
                match (table.get("regex"), table.get("prefix")) {
                    (Some(toml::Value::String(pattern)), _) => {
                        Some(NameMatch::Regex(pattern.clone()))
                    }
                    (_, Some(toml::Value::String(prefix))) => {
                        Some(NameMatch::Prefix(prefix.clone()))
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        match matcher {
            Some(matcher) => matchers.push(matcher),
            None => problems.push(format!(
                "`{key}` is a name, `{{ regex = \"...\" }}`, `{{ prefix = \"...\" }}`, or a list \
                 of them"
            )),
        }
    }
    matchers
}

fn parse_networks(value: &toml::Value, problems: &mut Vec<String>) -> Vec<Cidr> {
    let entries: Vec<&toml::Value> = match value {
        toml::Value::Array(entries) => entries.iter().collect(),
        single => vec![single],
    };
    let mut networks = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(text) = entry.as_str() else {
            problems.push("`address` is a network such as \"10.0.0.0/8\", or a list".to_owned());
            continue;
        };
        match Cidr::parse(text) {
            Ok(network) => networks.push(network),
            Err(CidrError::HostBits(network)) => problems.push(format!(
                "`address` {text:?} has bits set past its prefix; the network is \"{network}\""
            )),
            Err(CidrError::Malformed) => {
                problems.push(format!("`address` {text:?} is not an address or a network"))
            }
        }
    }
    networks
}

fn unknown_key(key: &str, known: &[&str], place: &str) -> String {
    match nearest(key, known) {
        Some(nearest) => format!("unknown key `{key}` in {place}; the nearest is `{nearest}`"),
        None => format!(
            "unknown key `{key}` in {place}; the keys are {}",
            known
                .iter()
                .map(|key| format!("`{key}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The known key closest to `key`, if it is close: within a third of its length in edits.
fn nearest<'a>(key: &str, known: &[&'a str]) -> Option<&'a str> {
    known
        .iter()
        .map(|candidate| (distance(key, candidate), *candidate))
        .filter(|(edits, candidate)| *edits <= candidate.len().div_ceil(3))
        .min_by_key(|(edits, _)| *edits)
        .map(|(_, candidate)| candidate)
}

/// The Levenshtein distance between two strings, by characters.
fn distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let substitute = diagonal + usize::from(ca != cb);
            diagonal = row[j + 1];
            row[j + 1] = substitute.min(row[j] + 1).min(diagonal + 1);
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn problems(text: &str) -> Vec<String> {
        match parse(text) {
            Err(Error::Acl { problems }) => problems,
            other => panic!("not refused: {other:?}"),
        }
    }

    #[test]
    fn a_rule_reads_into_its_parts() {
        let (rules, lines) = parse(
            r##"
version = 1

# Devices publish under their own name.
[[rule]]
permission = "allow"
username = ["exact", { regex = "dev-[0-9]+" }, { prefix = "svc:" }]
client_id = "c1"
address = ["10.0.0.0/8", "fd00::/8"]
attributes = { org = "acme", tier = { regex = "gold|silver" } }
action = "publish"
qos = [0, 1]
retain = false
topics = ["${username}/telemetry/#", { eq = "#" }, { all = true }]

[[rule]] # the end
permission = "deny"
action = "all"
qos = 2
topics = ["#"]
"##,
        )
        .unwrap();
        assert_eq!(lines, [Some(5), Some(16)]);
        let rule = &rules[0];
        assert_eq!(rule.decision, Decision::Allow);
        assert_eq!(
            rule.who.username.as_deref().unwrap(),
            [
                NameMatch::Exact("exact".into()),
                NameMatch::Regex("dev-[0-9]+".into()),
                NameMatch::Prefix("svc:".into()),
            ]
        );
        assert_eq!(rule.who.address.as_ref().unwrap().len(), 2);
        assert_eq!(rule.who.attributes.len(), 2);
        assert_eq!(rule.who.attributes[0].0, "org");
        assert_eq!(rule.action, ActionKind::Publish);
        assert_eq!(
            rule.qos,
            QosSet::EMPTY.with(QoS::AtMostOnce).with(QoS::AtLeastOnce)
        );
        assert_eq!(rule.retain, Some(false));
        assert_eq!(
            rule.topics,
            [
                TopicSpec::Filter("${username}/telemetry/#".into()),
                TopicSpec::Exact("#".into()),
                TopicSpec::All,
            ]
        );
        assert_eq!(rules[1].qos, QosSet::EMPTY.with(QoS::ExactlyOnce));
        assert!(rules[1].who.is_everyone());
    }

    #[test]
    fn every_problem_is_reported_by_rule_and_line() {
        let found = problems(
            r##"versoin = 1

[[rule]]
permission = "alow"
usrname = "x"
action = "publish"
qos = [3]
topics = [1]

[[rule]]
action = "everything"
address = "10.0.0.5/8"
username = { glob = "x*" }
"##,
        );
        assert_eq!(
            found,
            [
                "unknown key `versoin` in the file; the nearest is `version`",
                "rule 1 (line 3): unknown key `usrname` in a rule; the nearest is `username`",
                "rule 1 (line 3): `permission` is \"allow\" or \"deny\"",
                "rule 1 (line 3): `qos` lists QoS levels: 0, 1 or 2",
                "rule 1 (line 3): a topic is a filter string, `{ eq = \"...\" }` or `{ all = true }`",
                "rule 2 (line 10): `permission` is missing: \"allow\" or \"deny\"",
                "rule 2 (line 10): `action` is \"publish\", \"subscribe\" or \"all\"",
                "rule 2 (line 10): `topics` is missing",
                "rule 2 (line 10): `username` is a name, `{ regex = \"...\" }`, `{ prefix = \"...\" }`, or a list of them",
                "rule 2 (line 10): `address` \"10.0.0.5/8\" has bits set past its prefix; the network is \"10.0.0.0/8\"",
            ]
        );
        assert_eq!(
            problems("version = 2\n"),
            ["`version` is 1, the only version there is"]
        );
        assert_eq!(
            problems("rule = 3\n"),
            ["`rule` is an array of tables, written `[[rule]]`"]
        );
    }

    #[test]
    fn a_syntax_error_names_its_place_and_not_its_text() {
        let error = parse("[[rule]]\npermission = \"allow\nsecret = \"hunter2\"\n").unwrap_err();
        let Error::AclSyntax { line, column, .. } = &error else {
            panic!("{error:?}");
        };
        assert_eq!((*line, *column), (2, 20));
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }

    #[test]
    fn an_empty_file_has_no_rules() {
        assert_eq!(parse("").unwrap().0, []);
        assert_eq!(parse("version = 1\n").unwrap().0, []);
    }

    #[test]
    fn near_misses_are_named() {
        assert_eq!(nearest("client-id", &RULE_KEYS), Some("client_id"));
        assert_eq!(nearest("topic", &RULE_KEYS), Some("topics"));
        assert_eq!(nearest("zzzz", &RULE_KEYS), None);
        assert_eq!(distance("kitten", "sitting"), 3);
    }
}
