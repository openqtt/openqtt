//! `openqtt convert acl`: an `acl.conf` of OpenQTT 1.x into the ACL format of 2.0, deciding as
//! the original did except where report R2 says **Changed**.
//!
//! The 1.x file is the Erlang terms EMQX 5.8 reads ([acl.conf]), each rule
//! `{Permission, Who, Action, Topics}` or `{Permission, all}`. They are read with this crate's
//! own reader of the grammar, not EMQX's code. What a construct means is EMQX's, as its rule
//! module decides ([emqx_authz_rule.erl L139-L437][rule]), and `docs/spec/acl.md` says how each
//! is converted.
//!
//! [acl.conf]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_auth/etc/acl.conf
//! [rule]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_auth/src/emqx_authz/emqx_authz_rule.erl#L139-L437

use std::fmt::Write as _;

use regex::RegexBuilder;

use super::erlang::{self, Term};
use crate::Error;
use crate::acl::contract::{self, Contract};
use crate::acl::render::write_rule;
use crate::acl::spec::{
    ActionKind, Cidr, CidrError, Decision, NameMatch, QosSet, RuleSpec, TopicSpec, Who,
};
use crate::acl::{Acl, check_topic};

/// The most 2.0 rules one 1.x rule may become: each alternative of an `'or'` is a rule.
const MAX_ALTERNATIVES: usize = 64;

/// Something to know about one line of the 1.x file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    /// The line of the 1.x rule, counted from 1; 0 for the file as a whole.
    pub line: usize,
    /// What to know.
    pub message: String,
}

/// A converted `acl.conf`.
#[derive(Clone, Debug)]
pub struct AclConversion {
    /// The 2.0 ACL file.
    pub text: String,
    /// Its rules, in order, each with the line of the 1.x rule it came from.
    pub rules: Vec<(usize, RuleSpec)>,
    /// What the conversion changed or left out, by line.
    pub warnings: Vec<Note>,
    /// Rules that conflict with R2 rules 13 to 16, by line: what `--strict` refuses.
    pub conflicts: Vec<Note>,
}

/// What one 1.x rule became.
enum Converted {
    Rules(Vec<RuleSpec>),
    /// Left out: it matches nothing in 1.x either, or 2.0 refuses what it grants.
    Dropped {
        reason: String,
        r2_rule: Option<u8>,
    },
}

/// Converts `source`, an `acl.conf`, checking the result against `contract`.
///
/// # Errors
///
/// [`Error::Convert`] when the file is not Erlang terms; [`Error::Conversion`] with every rule
/// that is not one 1.x would load, or uses what 2.0 has no counterpart for, such as the
/// placeholder `${cert_common_name}`.
pub fn convert_acl(source: &str, contract: &Contract) -> Result<AclConversion, Error> {
    let commands: Vec<String> = contract
        .commands
        .iter()
        .filter(|filter| {
            filter.contains("${") || check_topic(&TopicSpec::Filter((*filter).clone())).is_err()
        })
        .map(|filter| format!("the command topics {filter:?} are not a topic filter"))
        .collect();
    if !commands.is_empty() {
        return Err(Error::Conversion { problems: commands });
    }
    let forms = erlang::parse(source)?;
    let mut text = String::from(
        "# Converted from an acl.conf of OpenQTT 1.x by `openqtt convert acl`. Each rule follows\n\
         # the 1.x rule it came from. The format is docs/spec/acl.md.\n\
         version = 1\n",
    );
    let mut rules = Vec::new();
    let mut warnings = Vec::new();
    let mut conflicts = Vec::new();
    let mut problems = Vec::new();
    for form in &forms {
        let line = form.line;
        let mut notes = Vec::new();
        let converted = convert_rule(&form.term, &mut notes);
        warnings.extend(notes.into_iter().map(|message| Note { line, message }));
        let _ = write!(text, "\n# line {line}: {}.\n", form.term);
        match converted {
            Ok(Converted::Rules(specs)) => {
                for (i, spec) in specs.into_iter().enumerate() {
                    if i > 0 {
                        text.push('\n');
                    }
                    write_rule(&spec, &mut text);
                    rules.push((line, spec));
                }
            }
            Ok(Converted::Dropped { reason, r2_rule }) => match r2_rule {
                Some(r2_rule) => {
                    let _ = writeln!(text, "# Not converted (R2 rule {r2_rule}): {reason}");
                    conflicts.push(Note {
                        line,
                        message: format!("R2 rule {r2_rule}: not converted: {reason}"),
                    });
                }
                None => {
                    let _ = writeln!(text, "# Not converted: {reason}");
                    warnings.push(Note {
                        line,
                        message: format!("not converted: {reason}"),
                    });
                }
            },
            Err(problem) => problems.push(format!("line {line}: {problem}")),
        }
    }
    if !problems.is_empty() {
        return Err(Error::Conversion { problems });
    }
    let specs: Vec<RuleSpec> = rules.iter().map(|(_, spec)| spec.clone()).collect();
    // What was converted must be a file 2.0 loads; anything else is a fault of this converter.
    Acl::compile(&specs)?;
    if !specs.last().is_some_and(catches_all) {
        text.push_str(
            "\n# Where no rule matches, 2.0 denies. 1.x applied `authorization.no_match` there,\n\
             # which allows unless it was set to deny.\n",
        );
        warnings.push(Note {
            line: 0,
            message: "the last rule does not match everything: where no rule matches, 2.0 denies, \
                      and 1.x applied `authorization.no_match`, which allows unless set to deny"
                .to_owned(),
        });
    }
    for conflict in contract::check(&specs, contract) {
        conflicts.push(Note {
            line: rules[conflict.rule].0,
            message: format!("R2 rule {}: {}", conflict.r2_rule, conflict.message),
        });
    }
    conflicts.sort_by_key(|note| note.line);
    Ok(AclConversion {
        text,
        rules,
        warnings,
        conflicts,
    })
}

/// Whether a rule matches every request of every client.
fn catches_all(rule: &RuleSpec) -> bool {
    rule.who.is_everyone()
        && rule.action == ActionKind::All
        && rule.qos == QosSet::ALL
        && rule.retain.is_none()
        && rule.topics.contains(&TopicSpec::All)
}

fn convert_rule(term: &Term, notes: &mut Vec<String>) -> Result<Converted, String> {
    let Term::Tuple(items) = term else {
        return Err(RULE_FORMS.to_owned());
    };
    let (decision, alternatives, (action, qos, retain), topics) = match items.as_slice() {
        [permission, Term::Atom(all)] if all == "all" => (
            decision(permission)?,
            vec![Who::default()],
            (ActionKind::All, QosSet::ALL, None),
            vec![TopicSpec::All],
        ),
        [permission, who_term, action_term, topics_term] => (
            decision(permission)?,
            who(who_term, notes)?,
            action(action_term, notes)?,
            topics(topics_term, notes)?,
        ),
        _ => return Err(RULE_FORMS.to_owned()),
    };
    let dropped = |reason: &str| {
        Ok(Converted::Dropped {
            reason: reason.to_owned(),
            r2_rule: None,
        })
    };
    if alternatives.is_empty() {
        return dropped("its clients' condition matches no client, in 1.x either");
    }
    if topics.is_empty() {
        return dropped("it has no topic that matches anything, in 1.x either");
    }
    if qos.is_empty() {
        return dropped("its action names no QoS, so it matches nothing, in 1.x either");
    }
    let mut specs = Vec::new();
    let mut address_alone = false;
    for who in alternatives {
        if decision == Decision::Allow && who.address.is_some() && !who.names_identity() {
            address_alone = true;
            continue;
        }
        specs.push(RuleSpec {
            decision,
            who,
            action,
            qos,
            retain,
            topics: topics.clone(),
        });
    }
    if address_alone {
        let reason = "an allow rule on the client's address alone, which grants nothing by \
                      itself in 2.0: name the clients as well";
        if specs.is_empty() {
            return Ok(Converted::Dropped {
                reason: reason.to_owned(),
                r2_rule: Some(14),
            });
        }
        notes.push(format!(
            "part of the rule is left out (R2 rule 14): {reason}"
        ));
    }
    Ok(Converted::Rules(specs))
}

const RULE_FORMS: &str = "a rule is {Permission, Who, Action, Topics} or {Permission, all}";

fn decision(term: &Term) -> Result<Decision, String> {
    match atom(term) {
        Some("allow") => Ok(Decision::Allow),
        Some("deny") => Ok(Decision::Deny),
        _ => Err("the permission is `allow` or `deny`".to_owned()),
    }
}

fn atom(term: &Term) -> Option<&str> {
    match term {
        Term::Atom(name) => Some(name),
        _ => None,
    }
}

/// The text of a string or a binary, which must be UTF-8.
fn text(term: &Term) -> Option<String> {
    match term {
        Term::Str(text) => Some(text.clone()),
        Term::Bin(bytes) => String::from_utf8(bytes.clone()).ok(),
        _ => None,
    }
}

/// The clients a condition names, as alternatives: the rule applies to a client when any of
/// them does, and to none when there are none.
fn who(term: &Term, notes: &mut Vec<String>) -> Result<Vec<Who>, String> {
    if atom(term) == Some("all") {
        return Ok(vec![Who::default()]);
    }
    let Term::Tuple(items) = term else {
        return Err(WHO_FORMS.to_owned());
    };
    let one = |who: Who| Ok(vec![who]);
    match items.as_slice() {
        [kind, value] if matches!(atom(kind), Some("user" | "username")) => one(Who {
            username: Some(vec![name(value, notes)?]),
            ..Who::default()
        }),
        [kind, value] if matches!(atom(kind), Some("client" | "clientid")) => one(Who {
            client_id: Some(vec![name(value, notes)?]),
            ..Who::default()
        }),
        [kind, value] if atom(kind) == Some("ipaddr") => one(Who {
            address: Some(vec![network(value, notes)?]),
            ..Who::default()
        }),
        [kind, Term::List(values)] if atom(kind) == Some("ipaddrs") => {
            if values.is_empty() {
                return Ok(Vec::new());
            }
            let networks = values
                .iter()
                .map(|value| network(value, notes))
                .collect::<Result<Vec<_>, _>>()?;
            one(Who {
                address: Some(networks),
                ..Who::default()
            })
        }
        [kind, attribute, value] if atom(kind) == Some("client_attr") => {
            let attribute = text(attribute).ok_or("a client attribute's name is a string")?;
            one(Who {
                attributes: vec![(attribute, vec![name(value, notes)?])],
                ..Who::default()
            })
        }
        [kind, Term::List(conditions)] if atom(kind) == Some("and") => {
            let mut alternatives = vec![Who::default()];
            for condition in conditions {
                let next = who(condition, notes)?;
                let mut merged = Vec::new();
                for left in &alternatives {
                    for right in &next {
                        if let Some(both) = merge(left, right)? {
                            merged.push(both);
                        }
                    }
                }
                if merged.len() > MAX_ALTERNATIVES {
                    return Err(TOO_MANY.to_owned());
                }
                alternatives = merged;
            }
            Ok(alternatives)
        }
        [kind, Term::List(conditions)] if atom(kind) == Some("or") => {
            let mut alternatives = Vec::new();
            for condition in conditions {
                alternatives.extend(who(condition, notes)?);
                if alternatives.len() > MAX_ALTERNATIVES {
                    return Err(TOO_MANY.to_owned());
                }
            }
            Ok(alternatives)
        }
        _ => Err(WHO_FORMS.to_owned()),
    }
}

const WHO_FORMS: &str = "the clients are `all`, {username, ...}, {clientid, ...}, {ipaddr, ...}, \
                         {ipaddrs, [...]}, {client_attr, Name, Value}, {'and', [...]} or \
                         {'or', [...]}";
const TOO_MANY: &str = "its 'and' and 'or' make more than 64 rules";

/// A name: `"text"`, `<<"text">>`, or `{re, "pattern"}`.
fn name(term: &Term, notes: &mut Vec<String>) -> Result<NameMatch, String> {
    if let Some(exact) = text(term) {
        return Ok(NameMatch::Exact(exact));
    }
    match term {
        Term::Tuple(items) => match items.as_slice() {
            [kind, pattern] if atom(kind) == Some("re") => {
                let pattern = text(pattern).ok_or("a pattern is a string")?;
                regex(&pattern, notes).map(NameMatch::Regex)
            }
            _ => Err("a name is a string, a binary, or {re, Pattern}".to_owned()),
        },
        _ => Err("a name is a string, a binary, or {re, Pattern}".to_owned()),
    }
}

/// A 1.x pattern, which `re:run` finds anywhere in the name, as a 2.0 pattern, which matches
/// the whole name.
///
/// `^X$` without an alternation becomes `X`. Anything else becomes `(?s:.*?)(?:X)(?s:.*)`,
/// which matches wherever `X` does, its anchors still at the ends of the name. One difference
/// remains: PCRE's `$` also matches before a newline that ends the name, and 2.0's does not.
fn regex(pattern: &str, notes: &mut Vec<String>) -> Result<String, String> {
    let trailing_backslashes = pattern
        .strip_suffix('$')
        .map(|rest| rest.len() - rest.trim_end_matches('\\').len());
    let anchored = pattern.len() >= 2
        && pattern.starts_with('^')
        && trailing_backslashes.is_some_and(|count| count % 2 == 0)
        && !pattern.contains('|');
    let converted = if anchored {
        pattern[1..pattern.len() - 1].to_owned()
    } else {
        format!("(?s:.*?)(?:{pattern})(?s:.*)")
    };
    if let Err(error) = RegexBuilder::new(&format!("^(?:{converted})$")).build() {
        let reason = error.to_string();
        let reason = reason
            .lines()
            .find(|line| !line.trim().is_empty())
            .unwrap_or(&reason);
        return Err(format!(
            "the pattern {pattern:?} is not one 2.0 can read: {}",
            reason.trim()
        ));
    }
    if ["\\d", "\\w", "\\s", "\\b", "\\D", "\\W", "\\S", "\\B"]
        .iter()
        .any(|class| pattern.contains(class))
    {
        notes.push(format!(
            "the pattern {pattern:?} uses a character class that is ASCII in 1.x and Unicode in \
             2.0, so a name with other scripts may match differently"
        ));
    }
    Ok(converted)
}

/// A network, as 1.x reads one: bits past the prefix are cleared.
fn network(term: &Term, notes: &mut Vec<String>) -> Result<Cidr, String> {
    let written = text(term).ok_or("an address is a string")?;
    match Cidr::parse(&written) {
        Ok(network) => Ok(network),
        Err(CidrError::HostBits(network)) => {
            notes.push(format!(
                "the address {written:?} has bits set past its prefix; 1.x read it as \
                 {network}, and so does the converted rule"
            ));
            Ok(network)
        }
        Err(CidrError::Malformed) => Err(format!("{written:?} is not an address or a network")),
    }
}

/// Both conditions at once, `None` when no client can meet both.
fn merge(left: &Who, right: &Who) -> Result<Option<Who>, String> {
    let Merged::Holds(username) =
        merge_names("user name", left.username.as_ref(), right.username.as_ref())?
    else {
        return Ok(None);
    };
    let Merged::Holds(client_id) = merge_names(
        "client identifier",
        left.client_id.as_ref(),
        right.client_id.as_ref(),
    )?
    else {
        return Ok(None);
    };
    let address = match (&left.address, &right.address) {
        (None, other) | (other, None) => other.clone(),
        (Some(xs), Some(ys)) => {
            let mut both = Vec::new();
            for &x in xs {
                for &y in ys {
                    let inner = if x.prefix() <= y.prefix() && x.contains(y.address()) {
                        Some(y)
                    } else if y.prefix() <= x.prefix() && y.contains(x.address()) {
                        Some(x)
                    } else {
                        None
                    };
                    if let Some(inner) = inner
                        && !both.contains(&inner)
                    {
                        both.push(inner);
                    }
                }
            }
            if both.is_empty() {
                return Ok(None);
            }
            Some(both)
        }
    };
    let mut attributes = left.attributes.clone();
    for (name, values) in &right.attributes {
        match attributes.iter_mut().find(|(existing, _)| existing == name) {
            Some((_, existing)) => {
                let Merged::Holds(merged) =
                    merge_names("client attribute", Some(&*existing), Some(values))?
                else {
                    return Ok(None);
                };
                *existing = merged.unwrap_or_default();
            }
            None => attributes.push((name.clone(), values.clone())),
        }
    }
    attributes.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Some(Who {
        username,
        client_id,
        address,
        attributes,
    }))
}

/// Two conditions on one name, as one.
enum Merged {
    /// No name meets both.
    Never,
    /// The condition both make, `None` for none.
    Holds(Option<Vec<NameMatch>>),
}

fn merge_names(
    what: &str,
    left: Option<&Vec<NameMatch>>,
    right: Option<&Vec<NameMatch>>,
) -> Result<Merged, String> {
    match (left, right) {
        (None, other) | (other, None) => Ok(Merged::Holds(other.cloned())),
        (Some(left), Some(right)) if left == right => Ok(Merged::Holds(Some(left.clone()))),
        (Some(left), Some(right)) => match (left.as_slice(), right.as_slice()) {
            ([NameMatch::Exact(_)], [NameMatch::Exact(_)]) => Ok(Merged::Never),
            _ => Err(format!(
                "an 'and' with two conditions on the {what} cannot be converted"
            )),
        },
    }
}

/// An action: `publish`, `subscribe`, `all`, or one of them with `[{qos, Q}, {retain, R}]`.
fn action(
    term: &Term,
    notes: &mut Vec<String>,
) -> Result<(ActionKind, QosSet, Option<bool>), String> {
    let kind = |term: &Term| match atom(term) {
        Some("publish") => Ok(ActionKind::Publish),
        Some("subscribe") => Ok(ActionKind::Subscribe),
        Some("all") => Ok(ActionKind::All),
        _ => Err("the action is publish, subscribe or all, or one of them with options".to_owned()),
    };
    let (kind, options) = match term {
        Term::Atom(_) => return Ok((kind(term)?, QosSet::ALL, None)),
        Term::Tuple(items) => match items.as_slice() {
            [action, Term::List(options)] => (kind(action)?, options),
            _ => return Err("an action with options is {Action, [Option]}".to_owned()),
        },
        _ => return Err("the action is publish, subscribe or all".to_owned()),
    };
    let mut qos = None;
    let mut retain: Option<Option<bool>> = None;
    for option in options {
        let Term::Tuple(pair) = option else {
            return Err("an action's option is {qos, Q} or {retain, R}".to_owned());
        };
        match pair.as_slice() {
            [key, value] if atom(key) == Some("qos") => {
                let levels: Vec<&Term> = match value {
                    Term::List(levels) => levels.iter().collect(),
                    level => vec![level],
                };
                let mut set = qos.unwrap_or(QosSet::EMPTY);
                for level in levels {
                    let level = match level {
                        Term::Int(level) => u8::try_from(*level)
                            .ok()
                            .and_then(openqtt_core::QoS::from_u8),
                        _ => None,
                    };
                    set = set.with(level.ok_or("a QoS is 0, 1 or 2")?);
                }
                qos = Some(set);
            }
            // The first `retain` decides, as 1.x reads it.
            [key, value] if atom(key) == Some("retain") => {
                let value = match atom(value) {
                    Some("true") => Some(true),
                    Some("false") => Some(false),
                    Some("all") => None,
                    _ => return Err("retain is true, false or all".to_owned()),
                };
                retain.get_or_insert(value);
            }
            _ => notes.push(format!(
                "the option {option} means nothing to 1.x and is left out"
            )),
        }
    }
    let retain = retain.flatten();
    if retain.is_some() && kind == ActionKind::Subscribe {
        notes.push("`retain` on a subscribe rule means nothing to 1.x and is left out".to_owned());
        return Ok((kind, qos.unwrap_or(QosSet::ALL), None));
    }
    Ok((kind, qos.unwrap_or(QosSet::ALL), retain))
}

/// The topics: `all`, or a list of filters, `"eq T"`, and `{eq, T}`.
fn topics(term: &Term, notes: &mut Vec<String>) -> Result<Vec<TopicSpec>, String> {
    if atom(term) == Some("all") {
        return Ok(vec![TopicSpec::All]);
    }
    let Term::List(items) = term else {
        return Err("the topics are a list, or `all`".to_owned());
    };
    let mut topics = Vec::with_capacity(items.len());
    for item in items {
        let topic = if let Some(filter) = text(item) {
            match filter.strip_prefix("eq ") {
                Some(exact) => TopicSpec::Exact(exact.to_owned()),
                None => {
                    placeholders(&filter)?;
                    TopicSpec::Filter(filter)
                }
            }
        } else {
            match item {
                Term::Tuple(pair) => match pair.as_slice() {
                    [kind, value] if atom(kind) == Some("eq") => {
                        TopicSpec::Exact(text(value).ok_or("{eq, Topic} holds a string")?)
                    }
                    _ => return Err("a topic is a string, a binary, or {eq, Topic}".to_owned()),
                },
                _ => return Err("a topic is a string, a binary, or {eq, Topic}".to_owned()),
            }
        };
        match check_topic(&topic) {
            Ok(()) => topics.push(topic),
            Err(reason) => notes.push(format!(
                "a topic that matches nothing, in 1.x either, is left out: {reason}"
            )),
        }
    }
    Ok(topics)
}

/// Refuses the placeholders 2.0 has no counterpart for.
fn placeholders(filter: &str) -> Result<(), String> {
    let mut rest = filter;
    while let Some(open) = rest.find("${") {
        let inside = &rest[open + 2..];
        let Some(close) = inside.find('}') else {
            return Err(format!(
                "the topic {filter:?} opens `${{` and does not close it, which 2.0 refuses"
            ));
        };
        match &inside[..close] {
            "username" | "clientid" => {}
            "cert_common_name" => {
                return Err(format!(
                    "the topic {filter:?} uses ${{cert_common_name}}, which 2.0 does not have: on \
                     a listener with certificate identity the CN is the user name, so write \
                     ${{username}}"
                ));
            }
            other => {
                return Err(format!(
                    "the topic {filter:?} uses ${{{other}}}, which 2.0 does not have; a topic may \
                     use ${{username}} and ${{clientid}}"
                ));
            }
        }
        rest = &inside[close + 1..];
    }
    Ok(())
}
