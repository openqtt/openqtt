//! What the differential harness compares with: traces line by line, the intended
//! divergences of report R1, and the identifiers R1 defines.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::Error;

/// Compares two traces as pretty-printed JSON. `None` when they are equal, else the lines
/// that differ, `-` for `expected` and `+` for `actual`, with a little context.
pub fn diff(expected: &Value, actual: &Value) -> Option<String> {
    if expected == actual {
        return None;
    }
    let pretty = |value: &Value| serde_json::to_string_pretty(value).unwrap_or_default();
    let (expected, actual) = (pretty(expected), pretty(actual));
    let left: Vec<&str> = expected.lines().collect();
    let right: Vec<&str> = actual.lines().collect();
    Some(line_diff(&left, &right))
}

/// One line of a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Line<'a> {
    Same(&'a str),
    Removed(&'a str),
    Added(&'a str),
}

/// A diff of two sequences of lines by their longest common subsequence, showing the lines
/// that differ with up to two lines of context.
fn line_diff(left: &[&str], right: &[&str]) -> String {
    // lengths[i][j]: the longest common subsequence of left[i..] and right[j..].
    let mut lengths = vec![vec![0_usize; right.len() + 1]; left.len() + 1];
    for i in (0..left.len()).rev() {
        for j in (0..right.len()).rev() {
            lengths[i][j] = if left[i] == right[j] {
                lengths[i + 1][j + 1] + 1
            } else {
                lengths[i + 1][j].max(lengths[i][j + 1])
            };
        }
    }
    let mut lines = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left.len() || j < right.len() {
        if i < left.len() && j < right.len() && left[i] == right[j] {
            lines.push(Line::Same(left[i]));
            i += 1;
            j += 1;
        } else if j < right.len() && (i == left.len() || lengths[i][j + 1] >= lengths[i + 1][j]) {
            lines.push(Line::Added(right[j]));
            j += 1;
        } else {
            lines.push(Line::Removed(left[i]));
            i += 1;
        }
    }
    let changed: Vec<bool> = lines
        .iter()
        .map(|line| !matches!(line, Line::Same(_)))
        .collect();
    let near_change = |index: usize| {
        let from = index.saturating_sub(2);
        let to = (index + 3).min(changed.len());
        changed[from..to].iter().any(|changed| *changed)
    };
    let mut out = String::new();
    let mut skipped = false;
    for (index, line) in lines.iter().enumerate() {
        if !near_change(index) {
            skipped = true;
            continue;
        }
        if skipped {
            out.push_str("  ...\n");
            skipped = false;
        }
        let (mark, text) = match line {
            Line::Same(text) => (' ', text),
            Line::Removed(text) => ('-', text),
            Line::Added(text) => ('+', text),
        };
        out.push(mark);
        out.push(' ');
        out.push_str(text);
        out.push('\n');
    }
    out
}

/// One of report R1's decisions that differ from EMQX, as the differential harness expects to
/// see it once OpenQTT 2.0 runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    /// The decision, `D1` to `D32`.
    pub id: String,
    /// The R1 statements it bears on, as `MQTT-x.y.z-n`; empty where it bears on none.
    pub statements: Vec<String>,
    /// What differs, in a sentence.
    pub summary: String,
    /// What EMQX 5.8.9, and so OpenQTT 1.x, does.
    pub emqx: String,
    /// What OpenQTT 2.0 does.
    pub openqtt: String,
    /// The scenarios whose traces show the difference; empty while none does.
    pub scenarios: Vec<String>,
}

/// Reads the `[[divergence]]` tables of a divergences file.
///
/// # Errors
///
/// [`Error::Toml`] when the file does not parse, or an entry lacks a field or has one of the
/// wrong type.
pub fn parse_divergences(text: &str) -> Result<Vec<Divergence>, Error> {
    let table: toml::Table = text
        .parse()
        .map_err(|error: toml::de::Error| Error::Toml(error.to_string()))?;
    let entries = match table.get("divergence") {
        Some(toml::Value::Array(entries)) => entries,
        _ => return Err(Error::Toml("no [[divergence]] tables".into())),
    };
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let entry = entry
                .as_table()
                .ok_or_else(|| Error::Toml(format!("divergence {index} is not a table")))?;
            let text = |key: &str| {
                entry
                    .get(key)
                    .and_then(toml::Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| Error::Toml(format!("divergence {index} has no string {key}")))
            };
            let list = |key: &str| -> Result<Vec<String>, Error> {
                let Some(value) = entry.get(key) else {
                    return Ok(Vec::new());
                };
                value
                    .as_array()
                    .ok_or_else(|| Error::Toml(format!("divergence {index}: {key} is not a list")))?
                    .iter()
                    .map(|item| {
                        item.as_str().map(str::to_owned).ok_or_else(|| {
                            Error::Toml(format!("divergence {index}: {key} holds a non-string"))
                        })
                    })
                    .collect()
            };
            Ok(Divergence {
                id: text("id")?,
                statements: list("statements")?,
                summary: text("summary")?,
                emqx: text("emqx")?,
                openqtt: text("openqtt")?,
                scenarios: list("scenarios")?,
            })
        })
        .collect()
}

/// The statement ids report R1 lists: the first cell of its table rows.
pub fn r1_statements(report: &str) -> BTreeSet<String> {
    report
        .lines()
        .filter_map(|line| line.strip_prefix("| MQTT-"))
        .filter_map(|rest| rest.split_once(' '))
        .map(|(id, _)| format!("MQTT-{id}"))
        .collect()
}

/// The decision ids report R1 defines: `D1` from its heading, the rest from the first cell of
/// its decision table.
pub fn r1_decisions(report: &str) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for line in report.lines() {
        let candidate = line
            .strip_prefix("### ")
            .and_then(|rest| rest.split_once('.'))
            .map(|(id, _)| id)
            .or_else(|| {
                line.strip_prefix("| ")
                    .and_then(|rest| rest.split_once(' '))
                    .map(|(id, _)| id)
            });
        if let Some(id) = candidate
            && let Some(number) = id.strip_prefix('D')
            && !number.is_empty()
            && number.bytes().all(|byte| byte.is_ascii_digit())
        {
            ids.insert(id.to_owned());
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn equal_traces_have_no_diff() {
        let trace = json!({ "scenario": "s", "clients": { "a": [1, 2, 3] } });
        assert_eq!(diff(&trace, &trace.clone()), None);
    }

    #[test]
    fn a_diff_shows_the_lines_that_differ() {
        let expected = json!({ "a": [1, 2, 3, 4, 5, 6, 7, 8] });
        let actual = json!({ "a": [1, 2, 3, 4, 50, 6, 7, 8] });
        let diff = diff(&expected, &actual).unwrap();
        assert!(diff.contains("-     5,"), "{diff}");
        assert!(diff.contains("+     50,"), "{diff}");
        // Lines far from the change are left out.
        assert!(!diff.contains("    1,"), "{diff}");
        assert!(diff.contains("..."), "{diff}");
    }

    #[test]
    fn divergences_parse_with_optional_lists() {
        let text = r#"
            [[divergence]]
            id = "D5"
            statements = ["MQTT-3.1.2-22"]
            summary = "Keep Alive"
            emqx = "between 1.5 and 2 times"
            openqtt = "at 1.5 times"
            scenarios = ["keepalive_timeout"]

            [[divergence]]
            id = "D28"
            summary = "Capabilities"
            emqx = "sent"
            openqtt = "left out"
        "#;
        let divergences = parse_divergences(text).unwrap();
        assert_eq!(divergences.len(), 2);
        assert_eq!(divergences[0].scenarios, ["keepalive_timeout"]);
        assert!(divergences[1].statements.is_empty());
        assert!(parse_divergences("[[divergence]]\nid = \"D1\"").is_err());
        assert!(parse_divergences("x = 1").is_err());
    }

    #[test]
    fn r1_ids_are_read_from_the_tables() {
        let report = "\
| MQTT-1.5.4-1 | String character data | codec |\n\
| MQTT-3.1.2-22 | Keep Alive | session |\n\
### D1. What a client of an older MQTT version receives\n\
| D2 | A denied PUBLISH | ... |\n\
| D32 | Topic syntax errors | ... |\n\
| Decision | not an id |\n";
        assert_eq!(
            r1_statements(report),
            BTreeSet::from(["MQTT-1.5.4-1".to_owned(), "MQTT-3.1.2-22".to_owned()])
        );
        assert_eq!(
            r1_decisions(report),
            BTreeSet::from(["D1".to_owned(), "D2".to_owned(), "D32".to_owned()])
        );
    }
}
