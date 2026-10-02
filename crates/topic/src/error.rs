//! The topic crate's one error type.

use crate::Placeholder;

/// Why a topic name, a topic filter or a mountpoint was refused, or a filter could not be
/// indexed.
///
/// Sections 4.7 and 4.8 bind the sender and name no reason code, so a topic that breaks them
/// does not have to end the connection ([MQTT-4.13.1-1]). Report R1 (O25) has the session
/// refuse the one item instead: 0x90 (Topic Name invalid) in the PUBACK or PUBREC of a PUBLISH
/// whose Topic Name or Response Topic is refused, and 0x8F (Topic Filter invalid) in the
/// SUBACK or UNSUBACK entry of a refused filter. The mapping is the session's, since this crate
/// knows no packets.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The topic is empty. Topic Names and Topic Filters are at least one character long
    /// ([MQTT-4.7.3-1]).
    #[error("a topic must be at least one character long")]
    Empty,
    /// The topic is longer than a UTF-8 Encoded String can be ([MQTT-4.7.3-3]).
    #[error("a topic must encode to at most 65,535 bytes, not {len}")]
    TooLong {
        /// Its length in bytes.
        len: usize,
    },
    /// The topic contains the null character ([MQTT-4.7.3-2]).
    #[error("a topic must not contain the null character U+0000")]
    NullCharacter,
    /// A Topic Name, or a Response Topic, contains a wildcard character ([MQTT-4.7.0-1],
    /// [MQTT-3.3.2-2], [MQTT-3.3.2-14]).
    #[error("a topic name must not contain the wildcard character {wildcard:?}")]
    WildcardInName {
        /// `+` or `#`.
        wildcard: char,
    },
    /// A `#` in a filter is not alone in the last level ([MQTT-4.7.1-1]).
    #[error("the multi-level wildcard `#` must be the last level of a filter, alone in it")]
    MisplacedMultiLevelWildcard,
    /// A `+` in a filter does not occupy a whole level ([MQTT-4.7.1-2]).
    #[error("the single-level wildcard `+` must occupy a whole level of a filter")]
    MisplacedSingleLevelWildcard,
    /// A shared subscription's ShareName is empty ([MQTT-4.8.2-1]).
    #[error("a shared subscription needs a ShareName of at least one character")]
    EmptyShareName,
    /// A ShareName contains `+` or `#` ([MQTT-4.8.2-2]).
    #[error("a ShareName must not contain `+` or `#`")]
    InvalidShareName,
    /// A ShareName is not followed by `/` and a topic filter ([MQTT-4.8.2-2]).
    #[error("a ShareName must be followed by `/` and a topic filter")]
    MissingSharedFilter,
    /// A mountpoint does not end with `/`, so it would not keep the levels of the topics put
    /// behind it whole.
    #[error("a mountpoint must end with `/`")]
    UnterminatedMountpoint,
    /// A mountpoint, as configured or once its placeholders have values, begins with
    /// `$share/`. An ordinary filter behind it would read as a shared subscription: behind
    /// `$share/g/`, the filter `t` becomes the text `$share/g/t`, which names the shared
    /// subscription to `t` in the group `g`.
    #[error("a mountpoint must not begin with `$share/`, which marks a shared subscription")]
    SharedMountpoint,
    /// A mountpoint names a placeholder other than `${username}` and `${clientid}`.
    #[error("a mountpoint may use ${{username}} and ${{clientid}}, not ${{{name}}}")]
    UnknownPlaceholder {
        /// The name between the braces.
        name: String,
    },
    /// A mountpoint opens a placeholder with `${` and never closes it.
    #[error("a mountpoint opens a placeholder with `${{` and does not close it")]
    UnclosedPlaceholder,
    /// The connection has no value for a placeholder its mountpoint uses: a client without a
    /// User Name under `${username}`.
    #[error("the connection has no value for {placeholder}")]
    MissingPlaceholderValue {
        /// The placeholder.
        placeholder: Placeholder,
    },
    /// A placeholder's value cannot go into a topic: it is empty, holds a wildcard character or
    /// U+0000, or would make the mounted topics begin with `$`.
    #[error("the value of {placeholder} cannot be used in a mountpoint")]
    InvalidPlaceholderValue {
        /// The placeholder.
        placeholder: Placeholder,
    },
    /// The index has run out of ids or of room for level text: about four billion nodes,
    /// terminals or levels, or 4 GiB of distinct level text.
    #[error("the topic index is full")]
    IndexFull,
}
