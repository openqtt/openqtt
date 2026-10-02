//! What each setting is, described once, from the declarations in `settings`.
//!
//! [`section!`] declares a section's struct, its defaults and its description in one list, so
//! the three cannot drift apart. Checking a file, mapping a variable to a key, suggesting the
//! nearest name for an unknown one, `openqtt config print` and the reference in
//! docs/spec/config.md all walk this description.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::OnceLock;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::settings::Settings;
use crate::values::{ByteSize, Duration, Endpoint, HostPort, SecretFile};

/// The prefix of every variable that is a setting.
pub(crate) const PREFIX: &str = "OPENQTT_";

/// One field of a section: a setting, a nested section, or sections the operator names.
pub(crate) struct Field {
    /// The key, as a file spells it.
    pub(crate) name: &'static str,
    /// The field's documentation comment, as the macro received it.
    pub(crate) doc: &'static str,
    /// What the field holds.
    pub(crate) node: Node,
}

/// What a field holds.
pub(crate) enum Node {
    /// One setting.
    Leaf(LeafInfo),
    /// A nested section.
    Section(Vec<Field>),
    /// Sections the operator names, each with the same fields: `[listeners.quic.<name>]`.
    Map {
        /// The fields of every entry.
        entry: Vec<Field>,
        /// One entry's defaults.
        entry_default: fn() -> toml::Value,
    },
}

/// How one setting's values are checked, read from a variable and described.
pub(crate) struct LeafInfo {
    /// The type, as the reference names it.
    pub(crate) type_name: String,
    /// Checks a value from the file or a variable; `Err` says why it does not fit.
    pub(crate) check: fn(&toml::Value) -> Result<(), String>,
    /// Reads a variable's text. `Ok(None)` unsets the setting.
    pub(crate) from_env: fn(&str) -> Result<Option<toml::Value>, String>,
}

/// A type one setting holds.
pub(crate) trait Leaf: Serialize + DeserializeOwned {
    /// The type, as the reference names it.
    fn type_name() -> String;

    /// The TOML value a variable's text stands for. Most types take the text as a string and
    /// leave judging it to their own parsing.
    fn from_env(text: &str) -> Result<toml::Value, String> {
        Ok(toml::Value::String(text.to_owned()))
    }
}

/// Anything a field holds.
pub(crate) trait Describe {
    /// The field's description.
    fn node() -> Node;
}

/// A section of the settings, declared with [`section!`].
pub(crate) trait Section: Default + Serialize {
    /// The section's fields, in the order they are declared.
    fn fields() -> Vec<Field>;
}

/// Declares a section of the settings: the struct, its defaults and its description, all from
/// one list. Every field takes a doc comment, which becomes its entry in the reference.
macro_rules! section {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $(
                $(#[doc = $doc:literal])*
                pub $field:ident: $ty:ty = $default:expr,
            )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
        #[serde(default, deny_unknown_fields)]
        #[non_exhaustive]
        pub struct $name {
            $(
                $(#[doc = $doc])*
                pub $field: $ty,
            )*
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $($field: $default,)*
                }
            }
        }

        impl $crate::schema::Section for $name {
            fn fields() -> Vec<$crate::schema::Field> {
                vec![$(
                    $crate::schema::Field {
                        name: stringify!($field),
                        doc: concat!($($doc, "\n",)*),
                        node: <$ty as $crate::schema::Describe>::node(),
                    },
                )*]
            }
        }

        impl $crate::schema::Describe for $name {
            fn node() -> $crate::schema::Node {
                $crate::schema::Node::Section(<Self as $crate::schema::Section>::fields())
            }
        }
    };
}

/// Declares a setting that takes one of a fixed set of words.
macro_rules! choice {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $(#[doc = $doc:literal])*
                $variant:ident = $text:literal,
            )+
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        pub enum $name {
            $(
                $(#[doc = $doc])*
                #[serde(rename = $text)]
                $variant,
            )+
        }

        impl $name {
            /// Every value, in the order the reference lists them.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            /// The value as the configuration spells it.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl $crate::schema::Leaf for $name {
            fn type_name() -> String {
                let words: Vec<String> =
                    Self::ALL.iter().map(|value| format!("`{value}`")).collect();
                format!("one of {}", words.join(", "))
            }
        }

        impl $crate::schema::Describe for $name {
            fn node() -> $crate::schema::Node {
                $crate::schema::leaf_node::<Self>()
            }
        }
    };
}

pub(crate) use {choice, section};

/// The description of a field holding one value of `T`.
pub(crate) fn leaf_node<T: Leaf>() -> Node {
    Node::Leaf(LeafInfo {
        type_name: T::type_name(),
        check: check::<T>,
        from_env: required::<T>,
    })
}

/// Checks a value by reading it as `T`, so a setting is judged by exactly the code that will
/// read it.
fn check<T: DeserializeOwned>(value: &toml::Value) -> Result<(), String> {
    T::deserialize(value.clone())
        .map(drop)
        .map_err(|error| first_line(&error.to_string()))
}

/// The first line of a message. toml appends the path of the value on a line of its own, and
/// the caller names the setting in its own words.
pub(crate) fn first_line(message: &str) -> String {
    message.lines().next().unwrap_or_default().trim().to_owned()
}

fn required<T: Leaf>(text: &str) -> Result<Option<toml::Value>, String> {
    T::from_env(text).map(Some)
}

/// An empty variable unsets an optional setting, since TOML has no null to say so.
fn optional<T: Leaf>(text: &str) -> Result<Option<toml::Value>, String> {
    if text.is_empty() {
        Ok(None)
    } else {
        T::from_env(text).map(Some)
    }
}

/// A list in a variable is comma-separated; an empty variable is an empty list.
fn list<T: Leaf>(text: &str) -> Result<Option<toml::Value>, String> {
    if text.trim().is_empty() {
        return Ok(Some(toml::Value::Array(Vec::new())));
    }
    text.split(',')
        .map(|item| T::from_env(item.trim()))
        .collect::<Result<Vec<_>, _>>()
        .map(|items| Some(toml::Value::Array(items)))
}

impl Leaf for String {
    fn type_name() -> String {
        "string".to_owned()
    }
}

impl Leaf for bool {
    fn type_name() -> String {
        "boolean".to_owned()
    }

    fn from_env(text: &str) -> Result<toml::Value, String> {
        match text {
            "true" => Ok(toml::Value::Boolean(true)),
            "false" => Ok(toml::Value::Boolean(false)),
            _ => Err(format!("expected `true` or `false`, found `{text}`")),
        }
    }
}

/// Integers read from a variable as TOML would read them; the type's own range is checked
/// when the value is.
macro_rules! integer_leaves {
    ($($ty:ty),*) => {
        $(
            impl Leaf for $ty {
                fn type_name() -> String {
                    "integer".to_owned()
                }

                fn from_env(text: &str) -> Result<toml::Value, String> {
                    text.parse::<i64>()
                        .map(toml::Value::Integer)
                        .map_err(|_| format!("expected a whole number, found `{text}`"))
                }
            }
        )*
    };
}

integer_leaves!(u16, u32, u64);

impl Leaf for PathBuf {
    fn type_name() -> String {
        "path".to_owned()
    }
}

impl Leaf for SocketAddr {
    fn type_name() -> String {
        "address, `ip:port`".to_owned()
    }
}

impl Leaf for Duration {
    fn type_name() -> String {
        "duration".to_owned()
    }
}

impl Leaf for ByteSize {
    fn type_name() -> String {
        "size".to_owned()
    }

    fn from_env(text: &str) -> Result<toml::Value, String> {
        Ok(match text.parse::<i64>() {
            Ok(bytes) => toml::Value::Integer(bytes),
            Err(_) => toml::Value::String(text.to_owned()),
        })
    }
}

impl Leaf for Endpoint {
    fn type_name() -> String {
        "URL".to_owned()
    }
}

impl Leaf for HostPort {
    fn type_name() -> String {
        "`host:port`".to_owned()
    }
}

impl Leaf for SecretFile {
    fn type_name() -> String {
        "path of a secret file".to_owned()
    }
}

/// Fields that hold one value of a leaf type.
macro_rules! leaves {
    ($($ty:ty),*) => {
        $(
            impl Describe for $ty {
                fn node() -> Node {
                    leaf_node::<$ty>()
                }
            }
        )*
    };
}

leaves!(
    String, bool, u16, u32, u64, PathBuf, SocketAddr, Duration, ByteSize, Endpoint, HostPort,
    SecretFile
);

impl<T: Leaf> Describe for Option<T> {
    fn node() -> Node {
        Node::Leaf(LeafInfo {
            type_name: T::type_name(),
            check: check::<T>,
            from_env: optional::<T>,
        })
    }
}

impl<T: Leaf> Describe for Vec<T> {
    fn node() -> Node {
        let item = T::type_name();
        let type_name = match item.strip_prefix("one of ") {
            Some(words) => format!("list, each one of {words}"),
            None => format!("list of {item}"),
        };
        Node::Leaf(LeafInfo {
            type_name,
            check: check::<Vec<T>>,
            from_env: list::<T>,
        })
    }
}

impl<S: Section> Describe for BTreeMap<String, S> {
    fn node() -> Node {
        Node::Map {
            entry: S::fields(),
            entry_default: section_default::<S>,
        }
    }
}

/// A section's defaults as TOML.
pub(crate) fn section_default<S: Section>() -> toml::Value {
    toml::Value::try_from(S::default())
        .expect("every value a section holds by default is one TOML can write")
}

/// The fields of [`Settings`], described once per process.
pub(crate) fn fields() -> &'static [Field] {
    static FIELDS: OnceLock<Vec<Field>> = OnceLock::new();
    FIELDS.get_or_init(<Settings as Section>::fields)
}

/// [`Settings::default`] as TOML.
pub(crate) fn defaults() -> &'static toml::Table {
    static DEFAULTS: OnceLock<toml::Table> = OnceLock::new();
    DEFAULTS.get_or_init(|| match section_default::<Settings>() {
        toml::Value::Table(table) => table,
        _ => toml::Table::new(),
    })
}

/// The table at `path` in `table`, if there is one.
pub(crate) fn table_at<'t>(table: &'t toml::Table, path: &[String]) -> Option<&'t toml::Table> {
    let mut current = table;
    for segment in path {
        current = current.get(segment)?.as_table()?;
    }
    Some(current)
}

/// The names of the entries of the map at `path`: those `table` defines, or else the defaults'.
/// A variable can change an entry that exists this way, never create one, so a misspelt name in
/// a variable is an unknown variable rather than a new listener.
pub(crate) fn entry_names(table: &toml::Table, path: &[String]) -> Vec<String> {
    table_at(table, path)
        .or_else(|| table_at(defaults(), path))
        .map(|entries| entries.keys().cloned().collect())
        .unwrap_or_default()
}

/// Whether `name` can name an entry of a map: words of lowercase letters and digits joined by
/// single underscores, so that it survives the trip through a variable's name.
pub(crate) fn is_entry_name(name: &str) -> bool {
    name.len() <= 63
        && name.split('_').all(|word| {
            !word.is_empty()
                && word
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
}

/// The setting `path` names, if it names one. Map entries must be among those `table` defines,
/// or the defaults' when it defines none.
pub(crate) fn leaf_at<'f>(
    fields: &'f [Field],
    path: &[String],
    table: &toml::Table,
) -> Option<&'f LeafInfo> {
    let mut fields = fields;
    let mut at = 0;
    while let Some(segment) = path.get(at) {
        let field = fields.iter().find(|field| field.name == segment)?;
        match &field.node {
            Node::Leaf(info) => return (at + 1 == path.len()).then_some(info),
            Node::Section(inner) => {
                fields = inner;
                at += 1;
            }
            Node::Map { entry, .. } => {
                let name = path.get(at + 1)?;
                if !entry_names(table, &path[..=at]).contains(name) {
                    return None;
                }
                fields = entry;
                at += 2;
            }
        }
    }
    None
}

/// Every key of the settings, with map entries named as in `table` or the defaults. Sections
/// are included when `sections` is set: a file can misspell one, a variable cannot name one.
pub(crate) fn key_paths(table: &toml::Table, sections: bool) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    collect_paths(fields(), &mut Vec::new(), table, sections, &mut out);
    out
}

fn collect_paths(
    fields: &[Field],
    prefix: &mut Vec<String>,
    table: &toml::Table,
    sections: bool,
    out: &mut Vec<Vec<String>>,
) {
    for field in fields {
        prefix.push(field.name.to_owned());
        match &field.node {
            Node::Leaf(_) => out.push(prefix.clone()),
            Node::Section(inner) => {
                if sections {
                    out.push(prefix.clone());
                }
                collect_paths(inner, prefix, table, sections, out);
            }
            Node::Map { entry, .. } => {
                if sections {
                    out.push(prefix.clone());
                }
                for name in entry_names(table, prefix) {
                    prefix.push(name);
                    if sections {
                        out.push(prefix.clone());
                    }
                    collect_paths(entry, prefix, table, sections, out);
                    prefix.pop();
                }
            }
        }
        prefix.pop();
    }
}

/// Calls `visit` with the path of every setting, and its value in `values` if it has one:
/// every setting of every section, and of every map entry `values` holds.
pub(crate) fn for_each_leaf(
    values: &toml::Table,
    visit: &mut dyn FnMut(&[String], Option<&toml::Value>),
) {
    walk_leaves(fields(), values, &mut Vec::new(), visit);
}

fn walk_leaves(
    fields: &[Field],
    values: &toml::Table,
    path: &mut Vec<String>,
    visit: &mut dyn FnMut(&[String], Option<&toml::Value>),
) {
    let empty = toml::Table::new();
    for field in fields {
        path.push(field.name.to_owned());
        let value = values.get(field.name);
        match &field.node {
            Node::Leaf(_) => visit(path, value),
            Node::Section(inner) => {
                let section = value.and_then(toml::Value::as_table).unwrap_or(&empty);
                walk_leaves(inner, section, path, visit);
            }
            Node::Map { entry, .. } => {
                let entries = value.and_then(toml::Value::as_table).unwrap_or(&empty);
                for (name, entry_values) in entries {
                    path.push(name.clone());
                    let section = entry_values.as_table().unwrap_or(&empty);
                    walk_leaves(entry, section, path, visit);
                    path.pop();
                }
            }
        }
        path.pop();
    }
}

/// The candidate nearest to `unknown`. Of two equally near, the one with the smaller edit
/// distance wins, then the first in order.
pub(crate) fn nearest(unknown: &[String], candidates: &[Vec<String>]) -> Option<Vec<String>> {
    candidates
        .iter()
        .min_by_key(|candidate| {
            let distance = levenshtein(&unknown.join("."), &candidate.join("."));
            (cost(unknown, candidate, distance), distance, *candidate)
        })
        .cloned()
}

/// How far `candidate` is from `unknown`: their edit `distance`, lowered for the mistakes
/// people make. A secret written inline beside its `_file` key is nearest of all; a key that
/// starts a sibling's name comes next; then the same key in another section, or under another
/// listener.
fn cost(unknown: &[String], candidate: &[String], distance: usize) -> usize {
    let (Some((last, parent)), Some((candidate_last, candidate_parent))) =
        (unknown.split_last(), candidate.split_last())
    else {
        return distance;
    };
    let siblings = parent == candidate_parent;
    if siblings && *candidate_last == format!("{last}_file") {
        0
    } else if siblings && !last.is_empty() && candidate_last.starts_with(last.as_str()) {
        distance.min(1)
    } else if last == candidate_last {
        distance.min(3)
    } else {
        distance
    }
}

/// The number of single-character insertions, deletions and substitutions between `a` and `b`.
fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, a) in a.chars().enumerate() {
        let mut diagonal = i;
        row[0] = i + 1;
        for (j, b) in b.iter().enumerate() {
            let substitution = diagonal + usize::from(a != *b);
            diagonal = row[j + 1];
            row[j + 1] = substitution.min(row[j] + 1).min(row[j + 1] + 1);
        }
    }
    row[b.len()]
}

/// The variable that sets the key at `path`: `["listeners", "quic", "default", "bind"]` is
/// `OPENQTT_LISTENERS__QUIC__DEFAULT__BIND`.
pub(crate) fn variable(path: &[impl AsRef<str>]) -> String {
    let segments: Vec<String> = path
        .iter()
        .map(|segment| segment.as_ref().to_ascii_uppercase())
        .collect();
    format!("{PREFIX}{}", segments.join("__"))
}

/// The key path a variable's name spells, when it spells one exactly as [`variable`] writes it.
/// Any other spelling, lowercase letters included, names no setting.
pub(crate) fn path_of(name: &str) -> Option<Vec<String>> {
    let rest = name.strip_prefix(PREFIX)?;
    let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
    (path.iter().all(|segment| !segment.is_empty()) && variable(&path) == name).then_some(path)
}

/// A doc comment as one paragraph: the macro keeps the space after `///` and the line breaks.
pub(crate) fn doc_text(doc: &str) -> String {
    doc.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(dotted: &str) -> Vec<String> {
        dotted.split('.').map(str::to_owned).collect()
    }

    #[test]
    fn edit_distance_counts_insertions_deletions_and_substitutions() {
        for (a, b, distance) in [
            ("", "", 0),
            ("", "abc", 3),
            ("abc", "", 3),
            ("bind", "bind", 0),
            ("certfile", "cert_file", 1),
            ("kitten", "sitting", 3),
            ("defualt", "default", 2),
        ] {
            assert_eq!(levenshtein(a, b), distance, "{a} {b}");
            assert_eq!(levenshtein(b, a), distance, "{b} {a}");
        }
    }

    #[test]
    fn the_nearest_key_fixes_the_usual_mistakes() {
        let candidates = key_paths(&toml::Table::new(), true);
        for (unknown, expected) in [
            // A typo in a key.
            (
                "listeners.quic.default.certfile",
                "listeners.quic.default.cert_file",
            ),
            ("observability.loglevel", "observability.log_level"),
            // A typo in a section or a listener's name.
            ("clustre", "cluster"),
            ("listeners.quic.defualt.bind", "listeners.quic.default.bind"),
            // A secret written inline, beside the key that names its file.
            (
                "listeners.quic.default.key",
                "listeners.quic.default.key_file",
            ),
            ("auth.password_bootstrap", "auth.password_bootstrap_file"),
            // The start of a name.
            ("cluster.node", "cluster.node_name"),
            // The right key in the wrong section.
            ("cluster.data_dir", "storage.data_dir"),
        ] {
            let nearest = nearest(&path(unknown), &candidates).unwrap();
            assert_eq!(nearest.join("."), expected, "{unknown}");
        }
    }

    #[test]
    fn variables_and_paths_map_both_ways_in_one_spelling() {
        let bind = path("listeners.quic.default.bind");
        assert_eq!(variable(&bind), "OPENQTT_LISTENERS__QUIC__DEFAULT__BIND");
        assert_eq!(
            path_of("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND"),
            Some(bind)
        );
        assert_eq!(
            path_of("OPENQTT_OBSERVABILITY__LOG_LEVEL"),
            Some(path("observability.log_level"))
        );
        for name in [
            "OPENQTT_",
            "OPENQTT_cluster__name",
            "OPENQTT_Cluster__Name",
            "OPENQTT_CLUSTER____NAME",
            "OPENQTT_CLUSTER__",
            "OTHER_CLUSTER__NAME",
        ] {
            assert_eq!(path_of(name), None, "{name}");
        }
    }

    #[test]
    fn entry_names_are_lowercase_words_joined_by_single_underscores() {
        for name in ["default", "devices", "devices_443", "a1"] {
            assert!(is_entry_name(name), "{name}");
        }
        let long = "a".repeat(64);
        for name in [
            "", "Default", "dev-ices", "dev ices", "_a", "a_", "a__b", "dé", &long,
        ] {
            assert!(!is_entry_name(name), "{name}");
        }
    }

    /// Every field of the settings with its path and description, map entries named `<name>`.
    fn every_field() -> Vec<(Vec<String>, &'static Field)> {
        fn walk(
            fields: &'static [Field],
            path: &mut Vec<String>,
            out: &mut Vec<(Vec<String>, &'static Field)>,
        ) {
            for field in fields {
                path.push(field.name.to_owned());
                out.push((path.clone(), field));
                match &field.node {
                    Node::Leaf(_) => {}
                    Node::Section(inner) => walk(inner, path, out),
                    Node::Map { entry, .. } => {
                        path.push("<name>".to_owned());
                        walk(entry, path, out);
                        path.pop();
                    }
                }
                path.pop();
            }
        }
        let mut out = Vec::new();
        walk(fields(), &mut Vec::new(), &mut out);
        out
    }

    #[test]
    fn every_secret_is_named_by_a_file_key_and_every_file_key_names_a_file() {
        let secret = <SecretFile as Leaf>::type_name();
        let path_type = <PathBuf as Leaf>::type_name();
        for (path, field) in every_field() {
            let Node::Leaf(leaf) = &field.node else {
                continue;
            };
            let is_secret = leaf.type_name == secret;
            let names_file = field.name.ends_with("_file");
            assert!(
                !is_secret || names_file,
                "{}: a secret's key ends in _file",
                path.join(".")
            );
            assert!(
                !names_file || is_secret || leaf.type_name == path_type,
                "{}: a key ending in _file holds a path",
                path.join(".")
            );
        }
    }

    #[test]
    fn every_setting_and_section_is_described() {
        for (path, field) in every_field() {
            assert!(
                !doc_text(field.doc).is_empty(),
                "{} has no description",
                path.join(".")
            );
        }
    }

    #[test]
    fn every_key_is_lowercase_words_and_its_variable_reads_back_to_it() {
        let mut variables = std::collections::BTreeSet::new();
        for path in key_paths(&toml::Table::new(), false) {
            for segment in &path {
                assert!(is_entry_name(segment), "{}: `{segment}`", path.join("."));
            }
            let name = variable(&path);
            assert_eq!(path_of(&name).as_ref(), Some(&path), "{name}");
            assert!(variables.insert(name.clone()), "{name} names two settings");
        }
        assert!(variables.len() > 50, "{} settings", variables.len());
    }

    #[test]
    fn a_doc_comment_reads_as_one_paragraph() {
        assert_eq!(
            doc_text(" First line,\n second line.\n"),
            "First line, second line."
        );
        assert_eq!(doc_text(""), "");
    }
}
