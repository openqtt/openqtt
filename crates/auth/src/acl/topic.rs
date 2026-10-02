//! A rule's topics: filters with placeholders, and how a subscription is checked against them.

use openqtt_core::TopicFilter;

/// A value a topic takes from the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Placeholder {
    /// `${username}`: the principal's user name.
    Username,
    /// `${clientid}`: the Client Identifier.
    ClientId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Text(Box<str>),
    Value(Placeholder),
}

/// A topic filter with placeholders, as a rule writes it: `${username}/telemetry/#`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Template {
    parts: Box<[Part]>,
}

/// Why a template cannot be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TemplateError {
    /// `${` with no `}` after it.
    Unclosed,
    /// A placeholder other than `${username}` and `${clientid}`.
    Unknown(String),
}

impl Template {
    /// Reads `text`, which may hold no placeholder at all.
    pub(crate) fn parse(text: &str) -> Result<Self, TemplateError> {
        let mut parts = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find("${") {
            if open > 0 {
                parts.push(Part::Text(rest[..open].into()));
            }
            let inside = &rest[open + 2..];
            let close = inside.find('}').ok_or(TemplateError::Unclosed)?;
            parts.push(Part::Value(match &inside[..close] {
                "username" => Placeholder::Username,
                "clientid" => Placeholder::ClientId,
                name => return Err(TemplateError::Unknown(name.to_owned())),
            }));
            rest = &inside[close + 1..];
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.into()));
        }
        Ok(Self {
            parts: parts.into_boxed_slice(),
        })
    }

    /// Whether it holds a placeholder.
    pub(crate) fn has_placeholders(&self) -> bool {
        self.parts.iter().any(|part| matches!(part, Part::Value(_)))
    }

    /// The filter for a client with `username` and `client_id`, or `None` when it has none:
    /// the client lacks a value it needs, a value is empty, holds `+`, `#` or U+0000, or would
    /// begin the filter with `$`, or the result is not a filter.
    ///
    /// The checks keep a client's own name from widening a rule: a user named `#` must not turn
    /// `${username}/x` into `#/x`, and a user named `$SYS` must not reach `$SYS/...`, which a
    /// rule has to name to grant (R2 rule 10).
    pub(crate) fn render(&self, username: Option<&str>, client_id: &str) -> Option<TopicFilter> {
        let mut text = String::new();
        for part in &*self.parts {
            match *part {
                Part::Text(ref literal) => text.push_str(literal),
                Part::Value(placeholder) => {
                    let value = match placeholder {
                        Placeholder::Username => username?,
                        Placeholder::ClientId => client_id,
                    };
                    let unusable = value.is_empty()
                        || value.bytes().any(|b| matches!(b, 0 | b'+' | b'#'))
                        || (text.is_empty() && value.starts_with('$'));
                    if unusable {
                        return None;
                    }
                    text.push_str(value);
                }
            }
        }
        let filter = TopicFilter::new(&text).ok()?;
        (!filter.is_shared()).then_some(filter)
    }
}

/// Whether an allow rule's filter `rule` allows the subscription filter `subscription`, both
/// without a `$share/{ShareName}/`: the subscription is read as a topic name and matched level by
/// level (R2 rule 11), so that a rule allowing `a/+/+` allows that filter and refuses broader
/// ones.
///
/// A literal level of the rule matches the same text only, so the rule's `b` does not allow
/// `+`. The rule's `+` matches any one level but `#`, which can stand for any number of levels,
/// the parent level included; only the rule's own `#` allows it. And a subscription beginning
/// with `$` is allowed only by a rule that names its first level, as `#` never matches a
/// `$`-topic (R2 rule 10, [MQTT-4.7.2-1]).
///
/// The check is level by level, and so errs towards refusal: the rule `+/#` matches every topic
/// `#` matches, and still does not allow the subscription `#`.
pub(crate) fn covers(rule: &str, subscription: &str) -> bool {
    levels_match(rule, subscription, false)
}

/// Whether a deny rule's filter `rule` refuses the subscription filter `subscription`: as
/// [`covers`], but the rule's `+` takes a `#` as well, as 1.x read it. A subscription to `a/#`
/// receives what `a/+` names, and a deny never refuses less than it did in 1.x.
pub(crate) fn denies(rule: &str, subscription: &str) -> bool {
    levels_match(rule, subscription, true)
}

fn levels_match(rule: &str, subscription: &str, plus_takes_hash: bool) -> bool {
    if subscription.starts_with('$') && rule.starts_with(['+', '#']) {
        return false;
    }
    let mut rule = rule.split('/');
    let mut subscription = subscription.split('/');
    loop {
        match (rule.next(), subscription.next()) {
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(level)) if plus_takes_hash || level != "#" => {}
            (Some(literal), Some(level)) if literal == level && literal != "+" => {}
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_read_their_placeholders() {
        let template = Template::parse("org/${username}/${clientid}-x/#").unwrap();
        assert!(template.has_placeholders());
        assert!(!Template::parse("$SYS/#").unwrap().has_placeholders());
        assert_eq!(Template::parse("a/${user"), Err(TemplateError::Unclosed));
        assert_eq!(
            Template::parse("${cert_common_name}/#"),
            Err(TemplateError::Unknown("cert_common_name".into()))
        );
        let filter = template.render(Some("acme/pump-3"), "c1").unwrap();
        assert_eq!(filter.as_str(), "org/acme/pump-3/c1-x/#");
    }

    #[test]
    fn a_client_value_never_widens_a_rule() {
        let template = Template::parse("${username}/x").unwrap();
        assert_eq!(template.render(Some("u"), "c").unwrap().as_str(), "u/x");
        for bad in ["", "#", "+", "a+b", "a#", "a\0", "$SYS", "$"] {
            assert_eq!(template.render(Some(bad), "c"), None, "{bad:?}");
        }
        assert_eq!(template.render(None, "c"), None);
        // `$` inside the filter is only text.
        let inside = Template::parse("in/${username}").unwrap();
        assert_eq!(inside.render(Some("$x"), "c").unwrap().as_str(), "in/$x");
        // A value cannot make a shared subscription either.
        let share = Template::parse("$share/${clientid}/t").unwrap();
        assert_eq!(share.render(None, "g"), None);
    }

    #[test]
    fn r2_rule_11_a_subscription_is_read_as_a_topic_name() {
        let rule = "ingest/acme/+/+/+";
        assert!(covers(rule, "ingest/acme/+/+/+"));
        assert!(covers(rule, "ingest/acme/a/b/c"));
        assert!(covers(rule, "ingest/acme/a/+/c"));
        // Broader filters are refused.
        for broader in [
            "ingest/acme/#",
            "ingest/+/+/+/+",
            "ingest/acme/+/+/#",
            "ingest/acme/+/+",
            "ingest/acme/+/+/+/+",
            "#",
        ] {
            assert!(!covers(rule, broader), "{broader}");
        }
        assert!(covers("a/#", "a"));
        assert!(covers("a/#", "a/#"));
        assert!(covers("a/#", "a/+/b/#"));
        assert!(covers("#", "#"));
        assert!(covers("+", "+"));
        assert!(!covers("a/b", "a/+"));
        assert!(!covers("a", "a/#"));
        // Level by level: `+/#` does not allow `#`, although it matches the same topics.
        assert!(!covers("+/#", "#"));
    }

    #[test]
    fn r2_rule_11_a_deny_refuses_what_it_refused_in_1x() {
        // The one difference: a deny's `+` takes a subscription's `#`.
        assert!(denies("a/+", "a/#"));
        assert!(!covers("a/+", "a/#"));
        assert!(denies("+", "#"));
        assert!(denies("a/+", "a/+"));
        assert!(!denies("a/b", "a/+"));
        assert!(!denies("#", "$SYS/#"));
    }

    #[test]
    fn r2_rule_10_dollar_topics_only_when_named() {
        assert!(!covers("#", "$SYS/#"));
        assert!(!covers("+/broker", "$SYS/broker"));
        assert!(covers("$SYS/#", "$SYS/broker/+"));
        assert!(!covers("$SYS/#", "+/broker"));
    }
}
