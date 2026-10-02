//! Topic Names (section 4.7).

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::Error;

/// The longest Topic Name or Topic Filter, in bytes: the most a UTF-8 Encoded String can hold
/// ([MQTT-4.7.3-3]).
pub const MAX_TOPIC_LEN: usize = 65_535;

/// A Topic Name: the topic of a PUBLISH, a Will Message or a retained message, or a Response
/// Topic, which a responder publishes to.
///
/// A name is at least one character long ([MQTT-4.7.3-1]), at most 65,535 bytes
/// ([MQTT-4.7.3-3]), and contains neither U+0000 ([MQTT-4.7.3-2]) nor a wildcard character
/// ([MQTT-4.7.0-1], [MQTT-3.3.2-2], [MQTT-3.3.2-14]). Its levels are separated by `/` and may be
/// empty, so `/finance`, `a//b` and `a/` are names. A name may begin with `$`: which clients may
/// publish to one is a question for authorization, and a filter starting with a wildcard never
/// matches one ([MQTT-4.7.2-1]).
///
/// Names compare, hash and order byte for byte, and are never normalized ([MQTT-4.7.3-4]). A
/// clone shares the text.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicName(Arc<str>);

impl TopicName {
    /// Checks `name` and takes a copy of it.
    ///
    /// # Errors
    ///
    /// [`Error::Empty`], [`Error::TooLong`], [`Error::NullCharacter`] or
    /// [`Error::WildcardInName`].
    pub fn new(name: &str) -> Result<Self, Error> {
        check_name(name)?;
        Ok(Self(name.into()))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The levels of the name, in order, empty ones included.
    pub fn levels(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }

    /// How many levels the name has, so that a limit on them can be applied before the
    /// mountpoint is (report R1, O16, and R2 rule 8).
    pub fn level_count(&self) -> usize {
        level_count(&self.0)
    }

    /// Whether the name begins with `$`, which a filter starting with a wildcard never matches
    /// ([MQTT-4.7.2-1]) and which section 4.7.2 reserves for the server's own use.
    pub fn starts_with_dollar(&self) -> bool {
        self.0.starts_with('$')
    }
}

/// The levels in `text`: one more than the separators in it.
pub(crate) fn level_count(text: &str) -> usize {
    text.bytes().filter(|&b| b == b'/').count() + 1
}

/// What names and filters share: at least one character ([MQTT-4.7.3-1]) and at most 65,535
/// bytes ([MQTT-4.7.3-3]).
pub(crate) fn check_length(text: &str) -> Result<(), Error> {
    if text.is_empty() {
        return Err(Error::Empty);
    }
    if text.len() > MAX_TOPIC_LEN {
        return Err(Error::TooLong { len: text.len() });
    }
    Ok(())
}

/// The rules for a Topic Name. Checking bytes is enough: `+`, `#` and U+0000 are ASCII, and
/// UTF-8 never uses an ASCII byte inside a longer character.
pub(crate) fn check_name(name: &str) -> Result<(), Error> {
    check_length(name)?;
    for &byte in name.as_bytes() {
        match byte {
            0 => return Err(Error::NullCharacter),
            b'+' | b'#' => {
                return Err(Error::WildcardInName {
                    wildcard: char::from(byte),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

impl fmt::Debug for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TopicName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TopicName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for TopicName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl FromStr for TopicName {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        Self::new(name)
    }
}

impl TryFrom<&str> for TopicName {
    type Error = Error;

    fn try_from(name: &str) -> Result<Self, Error> {
        Self::new(name)
    }
}

impl TryFrom<String> for TopicName {
    type Error = Error;

    fn try_from(name: String) -> Result<Self, Error> {
        check_name(&name)?;
        Ok(Self(name.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(name: &str) -> Error {
        TopicName::new(name).unwrap_err()
    }

    #[test]
    fn mqtt_4_7_0_1_a_topic_name_holds_no_wildcard() {
        for (name, wildcard) in [
            ("+", '+'),
            ("#", '#'),
            ("sport/+/player1", '+'),
            ("sport/tennis/#", '#'),
            ("sport+", '+'),
            ("a/b#c", '#'),
        ] {
            assert_eq!(refused(name), Error::WildcardInName { wildcard }, "{name}");
        }
        // The characters around them are fine.
        TopicName::new("sport/tennis/player1").unwrap();
        TopicName::new("$SYS/broker/load").unwrap();
    }

    #[test]
    fn mqtt_3_3_2_2_the_topic_name_of_a_publish_is_refused_with_a_wildcard() {
        assert_eq!(
            TopicName::try_from(String::from("devices/+/temperature")),
            Err(Error::WildcardInName { wildcard: '+' })
        );
        assert_eq!(
            "devices/#".parse::<TopicName>(),
            Err(Error::WildcardInName { wildcard: '#' })
        );
    }

    #[test]
    fn mqtt_3_3_2_14_a_response_topic_is_held_to_the_rules_of_a_topic_name() {
        // A Response Topic is the Topic Name of the response, so it is a TopicName: a wildcard
        // in it is refused, which the session answers with 0x90 (report R1, O25).
        for topic in ["replies/+", "replies/#", "+"] {
            assert!(matches!(
                TopicName::new(topic),
                Err(Error::WildcardInName { .. })
            ));
        }
        TopicName::new("replies/client-7").unwrap();
    }

    #[test]
    fn mqtt_4_7_3_1_a_topic_name_is_at_least_one_character() {
        assert_eq!(refused(""), Error::Empty);
        // One character is enough, even a separator, which makes two empty levels.
        let slash = TopicName::new("/").unwrap();
        assert_eq!(slash.levels().collect::<Vec<_>>(), ["", ""]);
        TopicName::new("a").unwrap();
    }

    #[test]
    fn mqtt_4_7_3_2_a_topic_name_holds_no_null_character() {
        assert_eq!(refused("\0"), Error::NullCharacter);
        assert_eq!(refused("a/\0/b"), Error::NullCharacter);
        assert_eq!(refused("a\0"), Error::NullCharacter);
    }

    #[test]
    fn mqtt_4_7_3_3_a_topic_name_encodes_to_at_most_65535_bytes() {
        let longest = "a".repeat(MAX_TOPIC_LEN);
        TopicName::new(&longest).unwrap();
        let too_long = "a".repeat(MAX_TOPIC_LEN + 1);
        assert_eq!(refused(&too_long), Error::TooLong { len: 65_536 });
        // Bytes count, not characters: 21,845 three-byte characters are 65,535 bytes.
        let wide = "\u{20AC}".repeat(21_845);
        assert_eq!(wide.len(), MAX_TOPIC_LEN);
        TopicName::new(&wide).unwrap();
        assert_eq!(refused(&format!("{wide}a")), Error::TooLong { len: 65_536 });
    }

    #[test]
    fn section_4_7_levels_may_be_empty() {
        for (name, levels) in [
            ("/finance", &["", "finance"][..]),
            ("a//b", &["a", "", "b"]),
            ("a/", &["a", ""]),
            ("sport", &["sport"]),
        ] {
            let topic = TopicName::new(name).unwrap();
            assert_eq!(topic.levels().collect::<Vec<_>>(), levels, "{name}");
            assert_eq!(topic.level_count(), levels.len(), "{name}");
        }
    }

    #[test]
    fn section_4_7_2_a_name_may_begin_with_a_dollar() {
        let system = TopicName::new("$SYS/monitor/Clients").unwrap();
        assert!(system.starts_with_dollar());
        assert!(!TopicName::new("a/$SYS").unwrap().starts_with_dollar());
    }

    #[test]
    fn names_are_kept_byte_for_byte() {
        // Neither case, nor Unicode normalization, nor a byte order mark changes a name.
        let composed = TopicName::new("caf\u{e9}").unwrap();
        let decomposed = TopicName::new("cafe\u{301}").unwrap();
        assert_ne!(composed, decomposed);
        assert_ne!(
            TopicName::new("ACCOUNTS").unwrap(),
            TopicName::new("Accounts").unwrap()
        );
        assert_eq!(TopicName::new("\u{feff}a").unwrap().as_str(), "\u{feff}a");
        assert_eq!(composed.to_string(), "caf\u{e9}");
        assert_eq!(format!("{composed:?}"), "TopicName(\"caf\u{e9}\")");
    }
}
