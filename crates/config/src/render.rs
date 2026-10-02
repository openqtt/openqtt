//! Writing settings back out: as TOML for `openqtt config print`, and as the reference tables of
//! docs/spec/config.md.

use std::fmt::Write as _;

use crate::schema::{self, Field, Node};
use crate::settings::Settings;

impl Settings {
    /// Every setting as TOML, defaults included, in the order they are declared: what `openqtt
    /// config print --effective` shows. A setting that is unset is written as a comment, so the
    /// text names every setting there is, and it loads back to these same settings.
    pub fn to_toml(&self) -> String {
        toml_text(&values(self), true)
    }
}

/// The settings as a TOML table.
pub(crate) fn values(settings: &Settings) -> toml::Table {
    match toml::Value::try_from(settings)
        .expect("every value a setting holds is one TOML can write")
    {
        toml::Value::Table(table) => table,
        _ => toml::Table::new(),
    }
}

/// `values` as TOML, in the order the settings are declared. With `unset`, a setting without a
/// value is written as a comment.
pub(crate) fn toml_text(values: &toml::Table, unset: bool) -> String {
    let mut out = String::new();
    section_text(&mut out, schema::fields(), values, &mut Vec::new(), unset);
    out
}

fn section_text(
    out: &mut String,
    fields: &[Field],
    values: &toml::Table,
    path: &mut Vec<String>,
    unset: bool,
) {
    let mut lines = Vec::new();
    for field in fields {
        if let Node::Leaf(_) = field.node {
            match values.get(field.name) {
                Some(value) => lines.push(format!("{} = {}", field.name, literal(value))),
                None if unset => lines.push(format!("# {} is unset", field.name)),
                None => {}
            }
        }
    }
    if !lines.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        if !path.is_empty() {
            let _ = writeln!(out, "[{}]", path.join("."));
        }
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }

    let empty = toml::Table::new();
    for field in fields {
        let value = values.get(field.name).and_then(toml::Value::as_table);
        match &field.node {
            Node::Leaf(_) => {}
            Node::Section(inner) => {
                path.push(field.name.to_owned());
                section_text(out, inner, value.unwrap_or(&empty), path, unset);
                path.pop();
            }
            Node::Map { entry, .. } => {
                path.push(field.name.to_owned());
                for (name, entry_values) in value.unwrap_or(&empty) {
                    path.push(name.clone());
                    let section = entry_values.as_table().unwrap_or(&empty);
                    section_text(out, entry, section, path, unset);
                    path.pop();
                }
                path.pop();
            }
        }
    }
}

/// A value as TOML writes it, on one line.
fn literal(value: &toml::Value) -> String {
    match value {
        toml::Value::String(text) => quoted(text),
        toml::Value::Integer(number) => number.to_string(),
        toml::Value::Float(number) if number.is_finite() && number.fract() == 0.0 => {
            format!("{number:.1}")
        }
        toml::Value::Float(number) => number.to_string(),
        toml::Value::Boolean(flag) => flag.to_string(),
        toml::Value::Datetime(datetime) => datetime.to_string(),
        toml::Value::Array(items) => {
            let items: Vec<String> = items.iter().map(literal).collect();
            format!("[{}]", items.join(", "))
        }
        toml::Value::Table(table) => {
            let entries: Vec<String> = table
                .iter()
                .map(|(key, value)| format!("{} = {}", quoted(key), literal(value)))
                .collect();
            format!("{{ {} }}", entries.join(", "))
        }
    }
}

/// A TOML basic string.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The reference tables of docs/spec/config.md: a heading for each section, then a row for each
/// of its settings with the type, the default, the variable and the description, all written
/// from the declarations in `settings.rs`. `make config-doc` writes them into the document, and
/// a test fails when the document and the declarations differ.
pub fn reference() -> String {
    let mut out = String::new();
    reference_section(
        &mut out,
        schema::fields(),
        schema::defaults(),
        &mut Vec::new(),
    );
    out
}

fn reference_section(
    out: &mut String,
    fields: &[Field],
    defaults: &toml::Table,
    path: &mut Vec<String>,
) {
    let empty = toml::Table::new();
    for field in fields {
        match &field.node {
            Node::Leaf(_) => {}
            Node::Section(inner) => {
                path.push(field.name.to_owned());
                let defaults = defaults
                    .get(field.name)
                    .and_then(toml::Value::as_table)
                    .unwrap_or(&empty);
                reference_table(out, field.doc, inner, defaults, path);
                reference_section(out, inner, defaults, path);
                path.pop();
            }
            Node::Map {
                entry,
                entry_default,
            } => {
                path.push(field.name.to_owned());
                path.push("<name>".to_owned());
                let entry_default = entry_default();
                let defaults = entry_default.as_table().unwrap_or(&empty);
                reference_table(out, field.doc, entry, defaults, path);
                reference_section(out, entry, defaults, path);
                path.pop();
                path.pop();
            }
        }
    }
}

/// One section's heading, description and table of settings.
fn reference_table(
    out: &mut String,
    doc: &str,
    fields: &[Field],
    defaults: &toml::Table,
    path: &[String],
) {
    if !out.is_empty() {
        out.push('\n');
    }
    let _ = writeln!(out, "### `[{}]`\n\n{}", path.join("."), cell(doc));
    let leaves: Vec<_> = fields
        .iter()
        .filter_map(|field| match &field.node {
            Node::Leaf(leaf) => Some((field, leaf)),
            _ => None,
        })
        .collect();
    if leaves.is_empty() {
        return;
    }
    out.push_str("\n| Key | Type | Default | Variable | Description |\n");
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for (field, leaf) in leaves {
        let default = defaults.get(field.name).map_or_else(
            || "unset".to_owned(),
            |value| format!("`{}`", literal(value)),
        );
        let mut key = path.to_vec();
        key.push(field.name.to_owned());
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | `{}` | {} |",
            field.name,
            cell(&leaf.type_name),
            cell(&default),
            schema::variable(&key),
            cell(field.doc),
        );
    }
}

/// Text for one table cell: one paragraph, with any `|` escaped.
fn cell(text: &str) -> String {
    schema::doc_text(text).replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_are_written_as_one_line_basic_strings() {
        assert_eq!(quoted("plain"), "\"plain\"");
        assert_eq!(quoted("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(quoted("one\ntwo\tthree\r"), "\"one\\ntwo\\tthree\\r\"");
        assert_eq!(quoted("\u{7}"), "\"\\u0007\"");
        let back: toml::Table =
            toml::from_str(&format!("x = {}", quoted("a\"b\\c\n\u{7}é"))).unwrap();
        assert_eq!(back["x"].as_str(), Some("a\"b\\c\n\u{7}é"));
    }

    #[test]
    fn every_kind_of_value_is_written_as_toml_reads_it() {
        let table: toml::Table = toml::from_str(
            "s = \"x\"\ni = -3\nf = 2.0\ng = 0.5\nb = true\na = [\"p\", 1]\nt = { k = 1 }\n",
        )
        .unwrap();
        let written: Vec<String> = table
            .iter()
            .map(|(key, value)| format!("{key} = {}", literal(value)))
            .collect();
        let back: toml::Table = toml::from_str(&written.join("\n")).unwrap();
        assert_eq!(back, table);
    }
}
