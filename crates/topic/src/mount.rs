//! Mountpoints (report R2, rule 6): a prefix put in front of every topic a client publishes and
//! every filter it subscribes, and taken off every topic delivered to it, so that each client
//! works in a namespace of its own.
//!
//! The production mountpoint is `ingest/${username}/`, where the username is the device's
//! certificate CN and contains `/` (R2, rule 4). A device publishing `temperature` is seen by
//! others at `ingest/<cn>/temperature`, and a retained `ingest/<cn>/commands/firmware` reaches
//! the device as `commands/firmware`.
//!
//! The session mounts a topic after authorization, which sees the client's own topics (R2, rule
//! 7), and after the limits of R2 rule 8; it strips on delivery. A Response Topic is neither
//! mounted nor stripped, since it reaches subscribers unaltered ([MQTT-3.3.2-15]).
//!
//! EMQX mounts the same way, by putting the mountpoint in front of the topic, and in front of
//! the filter of a shared subscription rather than its `$share/{ShareName}/` ([emqx_mountpoint.erl
//! L51-L95][mount]). It differs in what it accepts: it keeps a placeholder it has no value for
//! as literal text ([emqx_mountpoint.erl L105-L117][lookup]), which puts every client without a
//! username in one namespace, and delivers a topic outside the mountpoint unchanged. Here both
//! are refused.
//!
//! [mount]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_mountpoint.erl#L51-L95
//! [lookup]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_mountpoint.erl#L105-L117

use std::fmt;
use std::sync::Arc;

use crate::filter::SHARE_PREFIX;
use crate::name::check_name;
use crate::{Error, MAX_TOPIC_LEN, TopicFilter, TopicName};

/// A value a mountpoint takes from the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Placeholder {
    /// `${username}`: the User Name the client connected with, which on a listener configured
    /// for certificate identity is the certificate's CN (R2, rule 4).
    Username,
    /// `${clientid}`: the Client Identifier the session is kept under, assigned or not.
    ClientId,
}

impl Placeholder {
    /// The placeholder as a mountpoint writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Username => "${username}",
            Self::ClientId => "${clientid}",
        }
    }
}

impl fmt::Display for Placeholder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A mountpoint as configured, such as `ingest/${username}/`, before a connection gives its
/// placeholders their values.
///
/// It ends with `/`, so that a client's topics keep their levels whole behind it: without it,
/// `#` behind `ingest/u` would be `ingest/u#`, not a filter at all, and a client `dev1` could
/// publish `0/x` into the namespace of `dev10`. Its own text holds no wildcard character and no
/// U+0000, and its placeholders are `${username}` and `${clientid}`. It does not begin with
/// `$share/`, configured or resolved: behind `$share/g/`, the ordinary filter `t` would be the
/// text `$share/g/t`, which is the shared subscription to `t` in the group `g`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mountpoint {
    template: Box<str>,
    parts: Vec<Part>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Text(Box<str>),
    Value(Placeholder),
}

impl Mountpoint {
    /// Reads a mountpoint from configuration.
    ///
    /// # Errors
    ///
    /// [`Error::UnterminatedMountpoint`] unless it ends with `/`, an empty one included;
    /// [`Error::SharedMountpoint`] when it begins with `$share/`;
    /// [`Error::UnknownPlaceholder`] and [`Error::UnclosedPlaceholder`] for a placeholder it
    /// cannot read; [`Error::WildcardInName`], [`Error::NullCharacter`] or [`Error::TooLong`]
    /// for text that cannot begin a topic.
    pub fn parse(template: &str) -> Result<Self, Error> {
        let mut parts = Vec::new();
        let mut rest = template;
        while let Some(open) = rest.find("${") {
            if open > 0 {
                parts.push(Part::Text(rest[..open].into()));
            }
            let inside = &rest[open + 2..];
            let Some(close) = inside.find('}') else {
                return Err(Error::UnclosedPlaceholder);
            };
            let placeholder = match &inside[..close] {
                "username" => Placeholder::Username,
                "clientid" => Placeholder::ClientId,
                name => {
                    return Err(Error::UnknownPlaceholder {
                        name: name.to_owned(),
                    });
                }
            };
            parts.push(Part::Value(placeholder));
            rest = &inside[close + 1..];
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.into()));
        }
        for part in &parts {
            if let Part::Text(text) = part {
                check_name(text)?;
            }
        }
        if !template.ends_with('/') {
            return Err(Error::UnterminatedMountpoint);
        }
        if template.starts_with(SHARE_PREFIX) {
            return Err(Error::SharedMountpoint);
        }
        Ok(Self {
            template: template.into(),
            parts,
        })
    }

    /// The mountpoint as configured.
    pub fn as_str(&self) -> &str {
        &self.template
    }

    /// Whether the mountpoint takes a value for `placeholder` from the connection.
    pub fn uses(&self, placeholder: Placeholder) -> bool {
        self.parts.contains(&Part::Value(placeholder))
    }

    /// The mount for one connection: the mountpoint with the connection's User Name and Client
    /// Identifier in place of its placeholders.
    ///
    /// A value may contain `/`, which makes more levels, as production's usernames do. It must
    /// not be empty, which would put every client with that value in one namespace, nor contain
    /// a wildcard character or U+0000, nor begin the mount with `$`, which would move a
    /// client's topics among those that section 4.7.2 leaves to the server.
    ///
    /// # Errors
    ///
    /// [`Error::SharedMountpoint`] when the values make the mount begin with `$share/`,
    /// [`Error::MissingPlaceholderValue`] when the mountpoint uses `${username}` and the client
    /// sent no User Name, [`Error::InvalidPlaceholderValue`] for a value it cannot use, and
    /// [`Error::TooLong`] when no topic would fit behind the result.
    pub fn resolve(&self, username: Option<&str>, client_id: &str) -> Result<Mount, Error> {
        let mut prefix = String::new();
        for part in &self.parts {
            match *part {
                Part::Text(ref text) => prefix.push_str(text),
                Part::Value(placeholder) => {
                    let value = match placeholder {
                        Placeholder::Username => {
                            username.ok_or(Error::MissingPlaceholderValue { placeholder })?
                        }
                        Placeholder::ClientId => client_id,
                    };
                    let unusable = value.is_empty()
                        || value.bytes().any(|b| matches!(b, 0 | b'+' | b'#'))
                        || (prefix.is_empty() && value.starts_with('$'));
                    if unusable {
                        return Err(Error::InvalidPlaceholderValue { placeholder });
                    }
                    prefix.push_str(value);
                }
            }
        }
        // Text and values that are each fine can still spell `$share/` together.
        if prefix.starts_with(SHARE_PREFIX) {
            return Err(Error::SharedMountpoint);
        }
        // A mounted topic is at least one byte longer than the mount.
        if prefix.len() >= MAX_TOPIC_LEN {
            return Err(Error::TooLong {
                len: prefix.len() + 1,
            });
        }
        Ok(Mount(prefix.into()))
    }
}

impl fmt::Display for Mountpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.template)
    }
}

/// A mountpoint resolved for one connection: the literal prefix of its namespace, such as
/// `ingest/acme/production/pump-3/`. It ends with `/`, holds no wildcard character and no
/// U+0000, and does not begin with `$share/`, which is what keeps every mounted name and filter
/// valid and a mounted filter's text meaning what the filter does. A clone shares the text.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Mount(Arc<str>);

impl Mount {
    /// The prefix.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A topic the client publishes, as the rest of the cluster sees it.
    ///
    /// # Errors
    ///
    /// [`Error::TooLong`] when the mounted name would be over 65,535 bytes.
    pub fn mount_name(&self, name: &TopicName) -> Result<TopicName, Error> {
        let len = self.0.len() + name.as_str().len();
        if len > MAX_TOPIC_LEN {
            return Err(Error::TooLong { len });
        }
        let mut text = String::with_capacity(len);
        text.push_str(&self.0);
        text.push_str(name.as_str());
        Ok(TopicName::from_checked(&text))
    }

    /// A filter the client subscribes, as the rest of the cluster sees it. A shared
    /// subscription keeps its `$share/{ShareName}/` in front, and the mount goes before its
    /// pattern. Behind a mount ending in `/`, a wildcard still occupies whole levels.
    ///
    /// # Errors
    ///
    /// [`Error::TooLong`] when the mounted filter would be over 65,535 bytes.
    pub fn mount_filter(&self, filter: &TopicFilter) -> Result<TopicFilter, Error> {
        let len = self.0.len() + filter.as_str().len();
        if len > MAX_TOPIC_LEN {
            return Err(Error::TooLong { len });
        }
        let mut text = String::with_capacity(len);
        let pattern = match filter.share_name() {
            Some(share_name) => {
                text.push_str(SHARE_PREFIX);
                text.push_str(share_name);
                text.push('/');
                text.len()
            }
            None => 0,
        };
        text.push_str(&self.0);
        text.push_str(filter.pattern());
        Ok(TopicFilter::from_checked(&text, pattern))
    }

    /// A topic delivered to the client, as the client sees it: `name` with the mount taken off.
    ///
    /// `None` when `name` is not inside the namespace: when it does not begin with the mount,
    /// or is the mount itself, or is the mount without its final `/`, which a mounted `#`
    /// matches as its parent level. The session drops such a delivery rather than show the
    /// client a topic it could not have subscribed to.
    ///
    /// The session also delivers the stripped name only to a subscription whose own, unmounted
    /// filter matches it: the client's `#`, mounted as `ingest/u/#`, matches
    /// `ingest/u/$SYS/x`, but must not match the `$SYS/x` the client would see
    /// ([MQTT-4.7.2-1]).
    pub fn strip(&self, name: &TopicName) -> Option<TopicName> {
        let rest = name.as_str().strip_prefix(&*self.0)?;
        (!rest.is_empty()).then(|| TopicName::from_checked(rest))
    }
}

impl fmt::Display for Mount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> TopicName {
        TopicName::new(text).unwrap()
    }

    fn filter(text: &str) -> TopicFilter {
        TopicFilter::new(text).unwrap()
    }

    fn production(username: &str) -> Mount {
        Mountpoint::parse("ingest/${username}/")
            .unwrap()
            .resolve(Some(username), "client-1")
            .unwrap()
    }

    #[test]
    fn r2_rule_6_the_production_mountpoint() {
        // Usernames are certificate CNs, which contain `/`.
        let mount = production("acme/production/pump-3");
        assert_eq!(mount.as_str(), "ingest/acme/production/pump-3/");
        let published = mount.mount_name(&name("temperature")).unwrap();
        assert_eq!(
            published.as_str(),
            "ingest/acme/production/pump-3/temperature"
        );
        let subscribed = mount.mount_filter(&filter("commands/#")).unwrap();
        assert_eq!(
            subscribed.as_str(),
            "ingest/acme/production/pump-3/commands/#"
        );
        let retained = name("ingest/acme/production/pump-3/commands/firmware");
        assert!(subscribed.matches(&retained));
        assert_eq!(mount.strip(&retained), Some(name("commands/firmware")));
    }

    #[test]
    fn a_shared_subscription_is_mounted_behind_its_share_name() {
        let mount = production("acme/field/x");
        let shared = mount
            .mount_filter(&filter("$share/workers/jobs/+"))
            .unwrap();
        assert_eq!(shared.as_str(), "$share/workers/ingest/acme/field/x/jobs/+");
        assert_eq!(shared.share_name(), Some("workers"));
        assert_eq!(shared.pattern(), "ingest/acme/field/x/jobs/+");
        // A wildcard at the start of the client's filter still occupies a whole level.
        let all = mount.mount_filter(&filter("#")).unwrap();
        assert_eq!(all.as_str(), "ingest/acme/field/x/#");
        assert_eq!(TopicFilter::new(all.as_str()).unwrap(), all);
        let plus = mount.mount_filter(&filter("+/status")).unwrap();
        assert_eq!(plus.pattern(), "ingest/acme/field/x/+/status");
    }

    #[test]
    fn the_client_identifier_placeholder() {
        let mountpoint = Mountpoint::parse("devices/${clientid}/").unwrap();
        assert!(mountpoint.uses(Placeholder::ClientId));
        assert!(!mountpoint.uses(Placeholder::Username));
        // No username is needed when the mountpoint does not use one.
        let mount = mountpoint.resolve(None, "pump-3").unwrap();
        assert_eq!(mount.as_str(), "devices/pump-3/");
        // Both, and plain text, in any order.
        let both = Mountpoint::parse("t/${username}/${clientid}/x/")
            .unwrap()
            .resolve(Some("org"), "dev")
            .unwrap();
        assert_eq!(both.as_str(), "t/org/dev/x/");
        // A mountpoint without placeholders is the same prefix for every client.
        let fixed = Mountpoint::parse("$site/").unwrap();
        assert_eq!(fixed.resolve(None, "any").unwrap().as_str(), "$site/");
        assert_eq!(fixed.to_string(), "$site/");
    }

    #[test]
    fn strip_refuses_a_topic_outside_the_namespace() {
        let mount = production("acme/production/pump-3");
        for outside in [
            // Another device's namespace.
            "ingest/acme/production/pump-30/commands/firmware",
            "ingest/acme/production/pump-4/x",
            // The mount's parent level, which a mounted `#` matches.
            "ingest/acme/production/pump-3",
            // The mount itself, whose last level is empty: no topic the client publishes.
            "ingest/acme/production/pump-3/",
            "elsewhere",
        ] {
            assert_eq!(mount.strip(&name(outside)), None, "{outside}");
        }
        let all = mount.mount_filter(&filter("#")).unwrap();
        assert!(all.matches(&name("ingest/acme/production/pump-3")));
        // An empty level just inside the namespace is a topic the client can publish.
        assert_eq!(
            mount.strip(&name("ingest/acme/production/pump-3//x")),
            Some(name("/x"))
        );
    }

    #[test]
    fn mqtt_3_3_2_3_a_delivered_topic_matches_the_filter_the_client_subscribed() {
        // The session matches the message against mounted filters, strips the mount, and
        // delivers only to a subscription whose own filter matches what is left.
        let mount = production("o/n/d");
        let filters = [
            "#", "+", "+/+", "a/#", "a/+/c", "$SYS/#", "/+", "+/b/#", "a",
        ];
        let names = [
            "a",
            "a/b",
            "a/b/c",
            "/x",
            "$SYS/load",
            "$x",
            "b",
            "a/",
            "x/b/y",
        ];
        for f in filters {
            let client = filter(f);
            let mounted = mount.mount_filter(&client).unwrap();
            for n in names {
                let own = name(n);
                let published = mount.mount_name(&own).unwrap();
                // Nothing the client's filter matches is lost by mounting.
                if client.matches(&own) {
                    assert!(mounted.matches(&published), "{f} {n}");
                    assert_eq!(mount.strip(&published).as_ref(), Some(&own));
                }
                // What is delivered matches the client's filter.
                if mounted.matches(&published) {
                    let delivered = mount.strip(&published).unwrap();
                    let deliver = client.matches(&delivered);
                    // The mounted filter matches more only where the client's filter starts
                    // with a wildcard and the topic with `$` [MQTT-4.7.2-1].
                    assert_eq!(
                        deliver,
                        !(n.starts_with('$') && f.starts_with(['+', '#'])),
                        "{f} {n}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_mountpoint_ends_with_a_separator() {
        for template in ["", "ingest", "ingest/${username}", "${clientid}"] {
            assert_eq!(
                Mountpoint::parse(template),
                Err(Error::UnterminatedMountpoint),
                "{template:?}"
            );
        }
    }

    #[test]
    fn a_mountpoint_knows_two_placeholders() {
        assert_eq!(
            Mountpoint::parse("t/${zone}/"),
            Err(Error::UnknownPlaceholder {
                name: String::from("zone")
            })
        );
        assert_eq!(
            Mountpoint::parse("t/${client_attrs.org}/"),
            Err(Error::UnknownPlaceholder {
                name: String::from("client_attrs.org")
            })
        );
        assert_eq!(
            Mountpoint::parse("t/${username/"),
            Err(Error::UnclosedPlaceholder)
        );
        // A `$` that opens no placeholder is text, and so is a lone brace.
        let text = Mountpoint::parse("$t/a$b/{c}/").unwrap();
        assert_eq!(text.resolve(None, "x").unwrap().as_str(), "$t/a$b/{c}/");
        assert_eq!(
            Error::UnknownPlaceholder {
                name: String::from("zone")
            }
            .to_string(),
            "a mountpoint may use ${username} and ${clientid}, not ${zone}"
        );
    }

    #[test]
    fn a_mountpoint_holds_no_wildcard_or_null_character() {
        assert_eq!(
            Mountpoint::parse("t/+/"),
            Err(Error::WildcardInName { wildcard: '+' })
        );
        assert_eq!(
            Mountpoint::parse("t/${username}/#/"),
            Err(Error::WildcardInName { wildcard: '#' })
        );
        assert_eq!(Mountpoint::parse("t\0/"), Err(Error::NullCharacter));
    }

    #[test]
    fn a_mountpoint_cannot_begin_like_a_shared_subscription() {
        // Behind `$share/g/`, the ordinary filter `t` would be the text `$share/g/t`, which is
        // the shared subscription to `t` in group `g`: the same text with another meaning.
        for template in [
            "$share/g/",
            "$share/",
            "$share/${username}/",
            "$share/a/${clientid}/",
        ] {
            assert_eq!(
                Mountpoint::parse(template),
                Err(Error::SharedMountpoint),
                "{template}"
            );
        }
        // Text that only starts the same way is fine.
        for template in ["$shared/", "$share-x/", "a/$share/", "$SHARE/g/"] {
            Mountpoint::parse(template).unwrap();
        }
    }

    #[test]
    fn a_placeholder_cannot_make_the_mount_begin_like_a_shared_subscription() {
        let cases = [
            ("$${username}/", Some("share/g"), "c"),
            ("$share${username}/", Some("/g"), "c"),
            ("$shar${clientid}/", None, "e/g"),
        ];
        for (template, username, client_id) in cases {
            let mountpoint = Mountpoint::parse(template).unwrap();
            assert_eq!(
                mountpoint.resolve(username, client_id),
                Err(Error::SharedMountpoint),
                "{template}"
            );
        }
        // The same templates with other values resolve.
        let mount = Mountpoint::parse("$${username}/")
            .unwrap()
            .resolve(Some("x"), "c")
            .unwrap();
        assert_eq!(mount.as_str(), "$x/");
        let filter = mount.mount_filter(&filter("t")).unwrap();
        assert!(!filter.is_shared());
        assert_eq!(TopicFilter::new(filter.as_str()).unwrap(), filter);
    }

    #[test]
    fn a_placeholder_value_must_be_usable_in_a_topic() {
        let mountpoint = Mountpoint::parse("ingest/${username}/").unwrap();
        let username = Placeholder::Username;
        assert_eq!(
            mountpoint.resolve(None, "c"),
            Err(Error::MissingPlaceholderValue {
                placeholder: username
            })
        );
        for value in ["", "a+b", "a/#", "+", "a\0"] {
            assert_eq!(
                mountpoint.resolve(Some(value), "c"),
                Err(Error::InvalidPlaceholderValue {
                    placeholder: username
                }),
                "{value:?}"
            );
        }
        // `$` is an ordinary character below the first level.
        assert_eq!(
            mountpoint.resolve(Some("$x"), "c").unwrap().as_str(),
            "ingest/$x/"
        );
        // At the start of the mount it would move the client among the `$` topics.
        let leading = Mountpoint::parse("${clientid}/").unwrap();
        assert_eq!(
            leading.resolve(None, "$SYS"),
            Err(Error::InvalidPlaceholderValue {
                placeholder: Placeholder::ClientId
            })
        );
        assert_eq!(leading.resolve(None, "dev").unwrap().as_str(), "dev/");
    }

    #[test]
    fn a_mounted_topic_still_fits_in_65535_bytes() {
        let long = "a".repeat(65_534);
        let mountpoint = Mountpoint::parse("${clientid}/").unwrap();
        // The mount itself leaves no room for a topic.
        assert_eq!(
            mountpoint.resolve(None, &long),
            Err(Error::TooLong { len: 65_536 })
        );
        let mount = mountpoint.resolve(None, &long[..65_532]).unwrap();
        assert_eq!(mount.as_str().len(), 65_533);
        assert_eq!(
            mount.mount_name(&name("ab")).unwrap().as_str().len(),
            65_535
        );
        assert_eq!(
            mount.mount_name(&name("abc")),
            Err(Error::TooLong { len: 65_536 })
        );
        assert_eq!(
            mount.mount_filter(&filter("a/#")),
            Err(Error::TooLong { len: 65_536 })
        );
    }
}
