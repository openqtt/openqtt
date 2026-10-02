//! A reference interpreter for the tests: `docs/spec/acl.md` read as plainly as possible, over
//! the rules as written, with nothing compiled, bound or shared with the engine. The engine
//! must decide every case as this does.

use std::net::IpAddr;

use openqtt_ext::{Action, ClientInfo, Permission};
use regex::Regex;

use super::spec::{ActionKind, Decision, NameMatch, RuleSpec, TopicSpec, Who};

/// What an action asks, in the spec's words.
struct Asked<'a> {
    publishes: bool,
    qos: openqtt_core::QoS,
    retain: bool,
    /// The topic name, or the subscription's filter without `$share/{ShareName}/`.
    text: &'a str,
    is_filter: bool,
}

pub(crate) fn decide(rules: &[RuleSpec], client: &ClientInfo, action: &Action<'_>) -> Permission {
    let asked = match *action {
        Action::Publish {
            topic, qos, retain, ..
        } => Asked {
            publishes: true,
            qos,
            retain,
            text: topic.as_str(),
            is_filter: false,
        },
        Action::Subscribe { filter, qos, .. } => Asked {
            publishes: false,
            qos,
            retain: false,
            text: filter.pattern(),
            is_filter: true,
        },
        Action::Receive { topic, qos, .. } => Asked {
            publishes: false,
            qos,
            retain: false,
            text: topic.as_str(),
            is_filter: false,
        },
        _ => return Permission::Deny,
    };
    for rule in rules {
        let action_applies = if asked.publishes {
            rule.action != ActionKind::Subscribe && rule.retain.is_none_or(|r| r == asked.retain)
        } else {
            rule.action != ActionKind::Publish
        };
        if !action_applies || !rule.qos.contains(asked.qos) || !who(&rule.who, client) {
            continue;
        }
        if rule
            .topics
            .iter()
            .any(|topic| topic_applies(topic, client, &asked))
        {
            return match rule.decision {
                Decision::Allow => Permission::Allow,
                Decision::Deny => Permission::Deny,
            };
        }
    }
    Permission::Deny
}

fn who(who: &Who, client: &ClientInfo) -> bool {
    if let Some(matchers) = &who.username {
        let Some(name) = client.username() else {
            return false;
        };
        if !matchers.iter().any(|m| name_matches(m, name.as_str())) {
            return false;
        }
    }
    if let Some(matchers) = &who.client_id
        && !matchers
            .iter()
            .any(|m| name_matches(m, client.client_id.as_str()))
    {
        return false;
    }
    if let Some(networks) = &who.address {
        let Some(address) = client.address else {
            return false;
        };
        let (bits, width) = as_bits(address.ip());
        let inside = networks.iter().any(|network| {
            let (start, network_width) = as_bits(network.address());
            let prefix = u32::from(network.prefix());
            network_width == width && (prefix == 0 || (bits ^ start) >> (width - prefix) == 0)
        });
        if !inside {
            return false;
        }
    }
    who.attributes.iter().all(|(name, matchers)| {
        client
            .principal
            .attributes
            .get(name)
            .is_some_and(|value| matchers.iter().any(|m| name_matches(m, value)))
    })
}

/// An address as an integer and its width in bits, an IPv4 address written as IPv6 as IPv4.
fn as_bits(address: IpAddr) -> (u128, u32) {
    match address {
        IpAddr::V4(v4) => (u128::from(u32::from(v4)), 32),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => (u128::from(u32::from(v4)), 32),
            None => (u128::from(v6), 128),
        },
    }
}

fn name_matches(matcher: &NameMatch, name: &str) -> bool {
    match matcher {
        NameMatch::Exact(exact) => name == exact,
        NameMatch::Prefix(prefix) => name.starts_with(prefix.as_str()),
        NameMatch::Regex(pattern) => {
            Regex::new(&format!("^(?:{pattern})$")).is_ok_and(|regex| regex.is_match(name))
        }
    }
}

fn topic_applies(topic: &TopicSpec, client: &ClientInfo, asked: &Asked<'_>) -> bool {
    match topic {
        TopicSpec::All => true,
        TopicSpec::Exact(text) => asked.text == text,
        TopicSpec::Filter(template) => {
            let Some(filter) = render(template, client) else {
                return false;
            };
            let filter: Vec<&str> = filter.split('/').collect();
            let target: Vec<&str> = asked.text.split('/').collect();
            // A topic beginning with `$` is never matched by a filter beginning with a
            // wildcard.
            if asked.text.starts_with('$') && (filter[0] == "+" || filter[0] == "#") {
                return false;
            }
            if asked.is_filter {
                level_covers(&filter, &target)
            } else {
                level_matches(&filter, &target)
            }
        }
    }
}

/// The template with the client's values put in, or `None` where the spec says the topic
/// matches nothing for this client.
fn render(template: &str, client: &ClientInfo) -> Option<String> {
    let username = client.username().map(|name| name.as_str().to_owned());
    let client_id = client.client_id.as_str().to_owned();
    let mut out = String::new();
    let mut i = 0;
    let bytes = template.as_bytes();
    while i < bytes.len() {
        let rest = &template[i..];
        let value = if rest.starts_with("${username}") {
            i += "${username}".len();
            Some(username.clone()?)
        } else if rest.starts_with("${clientid}") {
            i += "${clientid}".len();
            Some(client_id.clone())
        } else {
            None
        };
        match value {
            Some(value) => {
                if value.is_empty()
                    || value.contains(['+', '#', '\0'])
                    || (out.is_empty() && value.starts_with('$'))
                {
                    return None;
                }
                out.push_str(&value);
            }
            None => {
                let c = rest.chars().next()?;
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    valid_filter(&out).then_some(out)
}

fn valid_filter(text: &str) -> bool {
    if text.is_empty() || text.len() > 65_535 || text.contains('\0') || text.starts_with("$share/")
    {
        return false;
    }
    let levels: Vec<&str> = text.split('/').collect();
    levels.iter().enumerate().all(|(i, level)| {
        let hash_ok = !level.contains('#') || (*level == "#" && i == levels.len() - 1);
        let plus_ok = !level.contains('+') || *level == "+";
        hash_ok && plus_ok
    })
}

fn level_matches(filter: &[&str], name: &[&str]) -> bool {
    match (filter.first(), name.first()) {
        (Some(&"#"), _) => true,
        (None, None) => true,
        (Some(&"+"), Some(_)) => level_matches(&filter[1..], &name[1..]),
        (Some(f), Some(n)) if f == n => level_matches(&filter[1..], &name[1..]),
        _ => false,
    }
}

/// R2 rule 11 as `docs/spec/acl.md` words it: the subscription's levels read as text; the
/// rule's `+` takes any one of them but `#`, and the rule's `#` takes the rest.
fn level_covers(rule: &[&str], subscription: &[&str]) -> bool {
    match (rule.first(), subscription.first()) {
        (Some(&"#"), _) => true,
        (None, None) => true,
        (Some(&"+"), Some(&level)) => level != "#" && level_covers(&rule[1..], &subscription[1..]),
        (Some(r), Some(s)) => r == s && level_covers(&rule[1..], &subscription[1..]),
        _ => false,
    }
}
