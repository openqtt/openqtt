//! Topic Filters (section 4.7), shared subscriptions (section 4.8), and matching a name against
//! a filter.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use crate::name::{check_length, level_count};
use crate::{Error, TopicName};

/// What a shared subscription's filter starts with ([MQTT-4.8.2-1]).
pub(crate) const SHARE_PREFIX: &str = "$share/";

/// A Topic Filter, as a SUBSCRIBE or UNSUBSCRIBE carries it: either a filter, or a shared
/// subscription `$share/{ShareName}/{filter}`.
///
/// A filter is at least one character long ([MQTT-4.7.3-1]), at most 65,535 bytes
/// ([MQTT-4.7.3-3]), and holds no U+0000 ([MQTT-4.7.3-2]). `#` stands alone in the last level
/// ([MQTT-4.7.1-1]) and `+` occupies a whole level ([MQTT-4.7.1-2]); levels may be empty. A
/// filter starting with `$share/` is a shared subscription, and then its ShareName is at least
/// one character ([MQTT-4.8.2-1]), holds no `+` or `#`, and is followed by `/` and a filter
/// ([MQTT-4.8.2-2]).
///
/// `$share/` is the only special form. `$queue/` and `$exclusive/`, which EMQX reads as a
/// shared and an exclusive subscription, are ordinary filters here (report R1, D17), and so is
/// a `$share/` filter inside a shared one, which EMQX refuses
/// ([emqx_topic.erl L280-L281](https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L280-L281)).
///
/// Filters compare, hash and order as the whole text, byte for byte, the way UNSUBSCRIBE
/// compares them ([MQTT-3.10.4-1]): `$queue/t` and `$share/$queue/t` are different filters. A
/// clone shares the text.
#[derive(Clone)]
pub struct TopicFilter {
    text: Arc<str>,
    /// Where the filter that names are matched against begins: 0, or the byte after
    /// `$share/{ShareName}/`.
    pattern: usize,
}

impl TopicFilter {
    /// Checks `filter` and takes a copy of it.
    ///
    /// # Errors
    ///
    /// [`Error::Empty`], [`Error::TooLong`] or [`Error::NullCharacter`] for any topic,
    /// [`Error::MisplacedMultiLevelWildcard`] or [`Error::MisplacedSingleLevelWildcard`] for a
    /// misplaced wildcard, and [`Error::EmptyShareName`], [`Error::InvalidShareName`] or
    /// [`Error::MissingSharedFilter`] for a malformed shared subscription.
    pub fn new(filter: &str) -> Result<Self, Error> {
        let pattern = check_filter(filter)?;
        Ok(Self {
            text: filter.into(),
            pattern,
        })
    }

    /// A filter built from parts that were each checked, with its pattern starting at
    /// `pattern`: a mountpoint and a filter, or a path through the index.
    pub(crate) fn from_checked(text: &str, pattern: usize) -> Self {
        Self {
            text: text.into(),
            pattern,
        }
    }

    /// The whole filter, as the client sent it, `$share/{ShareName}/` included.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Whether this is a shared subscription.
    pub fn is_shared(&self) -> bool {
        self.pattern > 0
    }

    /// The ShareName of a shared subscription.
    pub fn share_name(&self) -> Option<&str> {
        // The ShareName runs from the prefix to the `/` before the pattern.
        self.is_shared()
            .then(|| &self.text[SHARE_PREFIX.len()..self.pattern - 1])
    }

    /// The filter that topic names are matched against: the whole filter, or for a shared
    /// subscription what follows `$share/{ShareName}/`.
    pub fn pattern(&self) -> &str {
        &self.text[self.pattern..]
    }

    /// The levels of the pattern, in order, empty ones included.
    pub fn levels(&self) -> impl Iterator<Item = &str> {
        self.pattern().split('/')
    }

    /// How many levels the pattern has, so that a limit on them can be applied before the
    /// mountpoint is (report R1, O16).
    pub fn level_count(&self) -> usize {
        level_count(self.pattern())
    }

    /// Whether the pattern holds a wildcard, so that it can match more than one name.
    pub fn has_wildcard(&self) -> bool {
        self.pattern().contains(['+', '#'])
    }

    /// Whether this filter matches `name`, as section 4.7 defines it: `+` matches one level and
    /// `#` any number of levels, the parent level included; a pattern starting with a wildcard
    /// does not match a name starting with `$` ([MQTT-4.7.2-1]); and levels compare byte for
    /// byte ([MQTT-4.7.3-4]). A shared subscription matches with its pattern.
    pub fn matches(&self, name: &TopicName) -> bool {
        pattern_matches(self.pattern(), name.as_str())
    }
}

/// Matches a checked pattern against a checked name.
pub(crate) fn pattern_matches(pattern: &str, name: &str) -> bool {
    if name.starts_with('$') && pattern.starts_with(['+', '#']) {
        return false;
    }
    let mut patterns = pattern.split('/');
    let mut names = name.split('/');
    loop {
        match (patterns.next(), names.next()) {
            // `#` is always the last level, and matches what is left, even nothing.
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(_)) => {}
            (Some(level), Some(other)) if level == other => {}
            _ => return false,
        }
    }
}

/// Checks a whole filter and returns where its pattern begins.
pub(crate) fn check_filter(filter: &str) -> Result<usize, Error> {
    check_length(filter)?;
    if filter.as_bytes().contains(&0) {
        return Err(Error::NullCharacter);
    }
    let start = match filter.strip_prefix(SHARE_PREFIX) {
        None => 0,
        Some(rest) => {
            let Some(slash) = rest.find('/') else {
                return Err(if rest.is_empty() {
                    Error::EmptyShareName
                } else {
                    Error::MissingSharedFilter
                });
            };
            let share_name = &rest[..slash];
            if share_name.is_empty() {
                return Err(Error::EmptyShareName);
            }
            if share_name.contains(['+', '#']) {
                return Err(Error::InvalidShareName);
            }
            if slash + 1 == rest.len() {
                return Err(Error::MissingSharedFilter);
            }
            SHARE_PREFIX.len() + slash + 1
        }
    };
    check_pattern(&filter[start..])?;
    Ok(start)
}

/// The wildcard rules of section 4.7.1, for a pattern already free of U+0000.
fn check_pattern(pattern: &str) -> Result<(), Error> {
    let bytes = pattern.as_bytes();
    let last = bytes.len() - 1;
    for (i, &byte) in bytes.iter().enumerate() {
        let starts_level = i == 0 || bytes[i - 1] == b'/';
        let ends_level = i == last || bytes[i + 1] == b'/';
        match byte {
            b'#' if !(starts_level && i == last) => {
                return Err(Error::MisplacedMultiLevelWildcard);
            }
            b'+' if !(starts_level && ends_level) => {
                return Err(Error::MisplacedSingleLevelWildcard);
            }
            _ => {}
        }
    }
    Ok(())
}

impl PartialEq for TopicFilter {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl Eq for TopicFilter {}

impl Hash for TopicFilter {
    // The text alone, as `str` hashes it, so that `Borrow<str>` holds.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl PartialOrd for TopicFilter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopicFilter {
    fn cmp(&self, other: &Self) -> Ordering {
        self.text.cmp(&other.text)
    }
}

impl fmt::Debug for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TopicFilter").field(&self.as_str()).finish()
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl AsRef<str> for TopicFilter {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

impl Borrow<str> for TopicFilter {
    fn borrow(&self) -> &str {
        &self.text
    }
}

impl FromStr for TopicFilter {
    type Err = Error;

    fn from_str(filter: &str) -> Result<Self, Error> {
        Self::new(filter)
    }
}

impl TryFrom<&str> for TopicFilter {
    type Error = Error;

    fn try_from(filter: &str) -> Result<Self, Error> {
        Self::new(filter)
    }
}

impl TryFrom<String> for TopicFilter {
    type Error = Error;

    fn try_from(filter: String) -> Result<Self, Error> {
        let pattern = check_filter(&filter)?;
        Ok(Self {
            text: filter.into(),
            pattern,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(text: &str) -> TopicFilter {
        TopicFilter::new(text).unwrap()
    }

    fn name(text: &str) -> TopicName {
        TopicName::new(text).unwrap()
    }

    fn refused(text: &str) -> Error {
        TopicFilter::new(text).unwrap_err()
    }

    #[test]
    fn mqtt_4_7_0_1_wildcards_may_appear_in_filters() {
        for text in [
            "+",
            "#",
            "sport/+",
            "sport/#",
            "+/tennis/#",
            "+/+",
            "$SYS/#",
        ] {
            assert!(filter(text).has_wildcard(), "{text}");
        }
        assert!(!filter("sport/tennis").has_wildcard());
    }

    #[test]
    fn mqtt_4_7_1_1_the_multi_level_wildcard_stands_alone_in_the_last_level() {
        // The examples of section 4.7.1.2.
        for text in ["sport/tennis/player1/#", "sport/#", "#", "/#", "a//#"] {
            filter(text);
        }
        for text in [
            "sport/tennis#",
            "sport/tennis/#/ranking",
            "#/a",
            "a/#/",
            "a/b#",
            "##",
        ] {
            assert_eq!(refused(text), Error::MisplacedMultiLevelWildcard, "{text}");
        }
    }

    #[test]
    fn mqtt_4_7_1_2_the_single_level_wildcard_occupies_a_whole_level() {
        // The examples of section 4.7.1.3.
        for text in [
            "+",
            "+/tennis/#",
            "sport/+/player1",
            "/+",
            "+/",
            "+/+",
            "a/+/+/b",
        ] {
            filter(text);
        }
        for text in ["sport+", "+sport", "a/b+/c", "a/+b/c", "++", "a/++"] {
            assert_eq!(refused(text), Error::MisplacedSingleLevelWildcard, "{text}");
        }
    }

    #[test]
    fn mqtt_4_7_2_1_a_filter_starting_with_a_wildcard_does_not_match_a_dollar_topic() {
        // The examples of section 4.7.2.
        let system = name("$SYS/monitor/Clients");
        assert!(!filter("#").matches(&system));
        assert!(!filter("+/monitor/Clients").matches(&system));
        assert!(filter("$SYS/#").matches(&system));
        assert!(filter("$SYS/monitor/+").matches(&system));
        assert!(!filter("+").matches(&name("$SYS")));
        assert!(!filter("+/#").matches(&name("$x")));
        // Only the first level is special: below it, `$` is an ordinary character.
        assert!(filter("a/+").matches(&name("a/$b")));
        assert!(filter("a/#").matches(&name("a/$b/c")));
        // A shared subscription matches with its pattern, so the same rule applies.
        assert!(!filter("$share/g/#").matches(&system));
        assert!(filter("$share/g/$SYS/#").matches(&system));
    }

    #[test]
    fn mqtt_4_7_3_1_a_filter_is_at_least_one_character() {
        assert_eq!(refused(""), Error::Empty);
        filter("/");
        filter("+");
    }

    #[test]
    fn mqtt_4_7_3_2_a_filter_holds_no_null_character() {
        assert_eq!(refused("a/\0"), Error::NullCharacter);
        assert_eq!(refused("\0/#"), Error::NullCharacter);
        assert_eq!(refused("$share/g\0/a"), Error::NullCharacter);
        assert_eq!(refused("$share/g/a\0"), Error::NullCharacter);
    }

    #[test]
    fn mqtt_4_7_3_3_a_filter_encodes_to_at_most_65535_bytes() {
        let longest = format!("{}/#", "a".repeat(65_533));
        assert_eq!(longest.len(), 65_535);
        filter(&longest);
        assert_eq!(
            refused(&format!("a{longest}")),
            Error::TooLong { len: 65_536 }
        );
        // The limit is on the whole filter, `$share/{ShareName}/` included.
        let shared = format!("$share/g/{}", "a".repeat(65_526));
        assert_eq!(shared.len(), 65_535);
        filter(&shared);
        assert_eq!(
            refused(&format!("{shared}a")),
            Error::TooLong { len: 65_536 }
        );
    }

    #[test]
    fn mqtt_4_7_3_4_matching_neither_normalizes_nor_substitutes() {
        // Case, Unicode normalization and a byte order mark are all significant.
        assert!(!filter("ACCOUNTS").matches(&name("Accounts")));
        assert!(!filter("caf\u{e9}/+").matches(&name("cafe\u{301}/x")));
        assert!(filter("cafe\u{301}/+").matches(&name("cafe\u{301}/x")));
        assert!(!filter("\u{feff}a").matches(&name("a")));
        assert!(filter("\u{feff}a").matches(&name("\u{feff}a")));
        // Nor are trailing separators or empty levels folded away.
        assert!(!filter("a/b").matches(&name("a/b/")));
        assert!(!filter("a//b").matches(&name("a/b")));
        // A control character or a noncharacter is matched like any other.
        assert!(filter("a\tb/\u{fffe}").matches(&name("a\tb/\u{fffe}")));
    }

    #[test]
    fn section_4_7_1_matching_examples() {
        // Each case: filter, then names it matches, then names it does not.
        let cases: [(&str, &[&str], &[&str]); 8] = [
            (
                "sport/tennis/player1/#",
                &[
                    "sport/tennis/player1",
                    "sport/tennis/player1/ranking",
                    "sport/tennis/player1/score/wimbledon",
                ],
                &["sport/tennis/player2", "sport/tennis"],
            ),
            ("sport/#", &["sport", "sport/", "sport/a/b"], &["sports"]),
            ("#", &["sport", "/", "a/b/c"], &["$SYS"]),
            (
                "sport/tennis/+",
                &[
                    "sport/tennis/player1",
                    "sport/tennis/player2",
                    "sport/tennis/",
                ],
                &["sport/tennis/player1/ranking", "sport/tennis"],
            ),
            ("sport/+", &["sport/"], &["sport"]),
            ("+/+", &["/finance", "a/b", "/"], &["a", "a/b/c"]),
            ("/+", &["/finance", "/"], &["finance", "a/b"]),
            ("+", &["finance", "a"], &["/finance", "a/b"]),
        ];
        for (pattern, matching, other) in cases {
            let f = filter(pattern);
            for n in matching {
                assert!(f.matches(&name(n)), "{pattern} should match {n}");
            }
            for n in other {
                assert!(!f.matches(&name(n)), "{pattern} should not match {n}");
            }
        }
    }

    #[test]
    fn mqtt_4_8_2_1_a_shared_subscription_starts_with_share_and_names_its_group() {
        let shared = filter("$share/consumer1/sport/tennis/+");
        assert!(shared.is_shared());
        assert_eq!(shared.share_name(), Some("consumer1"));
        assert_eq!(shared.pattern(), "sport/tennis/+");
        assert_eq!(shared.as_str(), "$share/consumer1/sport/tennis/+");
        assert_eq!(shared.level_count(), 3);
        assert!(shared.matches(&name("sport/tennis/player1")));

        // The ShareName is at least one character long.
        assert_eq!(refused("$share//sport"), Error::EmptyShareName);
        assert_eq!(refused("$share/"), Error::EmptyShareName);
        // One character is enough, `$` included.
        assert_eq!(filter("$share/$/a").share_name(), Some("$"));

        // Anything else is an ordinary filter, whatever it starts with (report R1, D17).
        for text in [
            "$share",
            "$SHARE/g/a",
            "$queue/a",
            "$exclusive/a",
            "$shared/g/a",
        ] {
            let ordinary = filter(text);
            assert!(!ordinary.is_shared(), "{text}");
            assert_eq!(ordinary.share_name(), None);
            assert_eq!(ordinary.pattern(), text);
        }
        // So `$queue/t` and `$share/$queue/t` are two different subscriptions.
        let queue = filter("$queue/t");
        let shared_queue = filter("$share/$queue/t");
        assert_ne!(queue, shared_queue);
        assert_eq!(shared_queue.share_name(), Some("$queue"));
        assert_eq!(shared_queue.pattern(), "t");
    }

    #[test]
    fn mqtt_4_8_2_2_the_share_name_is_free_of_wildcards_and_followed_by_a_filter() {
        for text in ["$share/a+b/t", "$share/+/t", "$share/#/t", "$share/g#/t"] {
            assert_eq!(refused(text), Error::InvalidShareName, "{text}");
        }
        // A ShareName cannot hold `/`: the first one ends it, so it must be followed by a
        // filter.
        assert_eq!(refused("$share/group"), Error::MissingSharedFilter);
        assert_eq!(refused("$share/group/"), Error::MissingSharedFilter);
        // The filter follows the rules of section 4.7.
        assert_eq!(
            refused("$share/group/a#"),
            Error::MisplacedMultiLevelWildcard
        );
        assert_eq!(
            refused("$share/group/a+"),
            Error::MisplacedSingleLevelWildcard
        );
        // Any filter may follow, one beginning with `$` or `$share` included.
        assert_eq!(filter("$share/g/#").pattern(), "#");
        assert_eq!(filter("$share/g/$SYS/#").pattern(), "$SYS/#");
        let nested = filter("$share/g/$share/h/t");
        assert_eq!(nested.share_name(), Some("g"));
        assert_eq!(nested.pattern(), "$share/h/t");
    }

    #[test]
    fn filters_compare_as_their_whole_text() {
        use std::collections::HashSet;

        let a = filter("a/+");
        let set: HashSet<TopicFilter> = [a.clone(), filter("$share/g/a/+")].into();
        assert!(set.contains("a/+"));
        assert!(set.contains("$share/g/a/+"));
        assert!(!set.contains("$share/h/a/+"));
        assert!(filter("a") < filter("b"));
        assert_eq!(format!("{a:?}"), "TopicFilter(\"a/+\")");
        assert_eq!(a.to_string(), "a/+");
        assert_eq!(
            TopicFilter::try_from(String::from("$share/g/x")).unwrap(),
            filter("$share/g/x")
        );
    }
}
