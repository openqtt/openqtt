//! Reading the defaults, the file and the variables into [`Settings`].

use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::Error;
use crate::render;
use crate::schema::{self, Field, Node, PREFIX};
use crate::settings::Settings;

/// The variable naming the configuration file, when the command line's `--config` does not.
pub const CONFIG_VAR: &str = "OPENQTT_CONFIG";

/// Variables that named settings before the settings had their present names, and the
/// variable to set now. An unknown variable's message names the new one.
const RENAMED: [(&str, &str); 2] = [
    // The log filter, read on its own before there were settings.
    ("OPENQTT_LOG", "OPENQTT_OBSERVABILITY__LOG_LEVEL"),
    // Report R3's name for the seeds.
    ("OPENQTT_SEEDS", "OPENQTT_CLUSTER__SEEDS"),
];

/// Where settings come from: the command line's `--config`, if it gave one, and the variables
/// of the environment that are OpenQTT's to read, those starting `OPENQTT_` or `OTEL_`.
#[derive(Clone, Default)]
pub struct Sources {
    config: Option<PathBuf>,
    vars: Vec<(OsString, OsString)>,
}

impl Sources {
    /// The process's own: its variables, read here, once, and `config`, the path the command
    /// line gave with `--config`.
    pub fn from_process(config: Option<PathBuf>) -> Self {
        #[expect(
            clippy::disallowed_methods,
            reason = "openqtt-config is the one crate that reads the environment"
        )]
        let vars = std::env::vars_os();
        Self::new(config, vars)
    }

    /// Sources given outright: `config` as `--config` would give it, and `vars` as the
    /// environment would hold them. Variables that are not OpenQTT's are dropped.
    pub fn new<I, K, V>(config: Option<PathBuf>, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        let mut vars: Vec<(OsString, OsString)> = vars
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .filter(|(name, _)| {
                let name = name.to_string_lossy();
                name.starts_with(PREFIX) || name.starts_with("OTEL_")
            })
            .collect();
        // By name, so problems are reported in the same order whatever order the environment
        // keeps its variables in.
        vars.sort();
        Self { config, vars }
    }
}

/// Names only: a variable's value may be a secret set by mistake.
impl fmt::Debug for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sources")
            .field("config", &self.config)
            .field(
                "vars",
                &self.vars.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Settings as loaded, with what the file and the variables set.
#[derive(Debug, Clone)]
pub struct Loaded {
    settings: Settings,
    file: Option<PathBuf>,
    explicit: toml::Table,
}

impl Loaded {
    /// The settings.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The settings, taken out.
    pub fn into_settings(self) -> Settings {
        self.settings
    }

    /// The file the settings were read from, if one was.
    pub fn file(&self) -> Option<&Path> {
        self.file.as_deref()
    }

    /// What the file and the variables set, as TOML, without the defaults: what `openqtt config
    /// print` shows without `--effective`.
    pub fn explicit_toml(&self) -> String {
        render::toml_text(&self.explicit, false)
    }
}

impl Settings {
    /// Reads the settings: the defaults, then the file `--config` or [`CONFIG_VAR`] names, then
    /// the `OPENQTT_` variables.
    ///
    /// This checks every key and value, but not how settings go together, nor that the files
    /// they name exist: [`Settings::validate`] and [`Settings::check_files`] do that.
    ///
    /// # Errors
    ///
    /// Every problem found, in one [`Error`]: an unreadable or malformed file, an unknown key,
    /// an unknown `OPENQTT_` variable, any `OTEL_` variable, or a value its setting refuses.
    pub fn load(sources: &Sources) -> Result<Loaded, Error> {
        let mut problems = Vec::new();
        let mut config_var = None;
        let mut vars = Vec::new();
        for (name, value) in &sources.vars {
            let Some(name) = name.to_str() else {
                problems.push(unknown_variable(
                    &name.to_string_lossy(),
                    &toml::Table::new(),
                ));
                continue;
            };
            if name.starts_with("OTEL_") {
                problems.push(otel_variable(name));
                continue;
            }
            let Some(value) = value.to_str() else {
                problems.push(Error::NotUnicode {
                    name: name.to_owned(),
                });
                continue;
            };
            if name == CONFIG_VAR {
                config_var = (!value.is_empty()).then(|| PathBuf::from(value));
            } else {
                vars.push((name.to_owned(), value.to_owned()));
            }
        }

        let file = sources.config.clone().or(config_var);
        let mut table = match &file {
            None => toml::Table::new(),
            Some(path) => match read_file(path) {
                Ok(read) => {
                    let fields = schema::fields();
                    check_table(&read, fields, &mut Vec::new(), &read, path, &mut problems);
                    read
                }
                Err(error) => {
                    // Without the file the variables cannot be judged, since which listeners
                    // exist depends on it.
                    problems.push(error);
                    return Err(Error::from_problems(problems));
                }
            },
        };

        for (name, value) in &vars {
            let found = schema::path_of(name).and_then(|path| {
                schema::leaf_at(schema::fields(), &path, &table).map(|leaf| (path, leaf))
            });
            let Some((path, leaf)) = found else {
                problems.push(unknown_variable(name, &table));
                continue;
            };
            let read = (leaf.from_env)(value).and_then(|read| match read {
                Some(read) => (leaf.check)(&read).map(|()| Some(read)),
                None => Ok(None),
            });
            match read {
                Ok(Some(read)) => insert_at(&mut table, &path, read),
                Ok(None) => remove_at(&mut table, &path),
                Err(reason) => problems.push(Error::Value {
                    place: name.clone(),
                    reason,
                }),
            }
        }
        Error::check(problems)?;

        let settings =
            Settings::deserialize(toml::Value::Table(table.clone())).map_err(|error| {
                Error::Value {
                    place: "the configuration".to_owned(),
                    reason: schema::first_line(&error.to_string()),
                }
            })?;
        Ok(Loaded {
            settings,
            file,
            explicit: table,
        })
    }

    /// Checks that every file a `*_file` setting names can be opened. `openqtt config check`
    /// runs this; reading what the files hold is left to the code that uses them.
    ///
    /// # Errors
    ///
    /// One [`Error::File`] for each file that cannot be opened, or is a directory.
    pub fn check_files(&self) -> Result<(), Error> {
        let mut problems = Vec::new();
        let values = render::values(self);
        schema::for_each_leaf(&values, &mut |path, value| {
            let is_file = path.last().is_some_and(|key| key.ends_with("_file"));
            if let (true, Some(toml::Value::String(file))) = (is_file, value)
                && let Err(source) = openable(Path::new(file))
            {
                problems.push(Error::File {
                    key: path.join("."),
                    path: PathBuf::from(file),
                    source,
                });
            }
        });
        Error::check(problems)
    }
}

/// Opens the file, which proves the process may read it, and refuses a directory.
fn openable(path: &Path) -> std::io::Result<()> {
    if File::open(path)?.metadata()?.is_dir() {
        return Err(std::io::Error::other("it is a directory"));
    }
    Ok(())
}

/// The file's table, or why it has none.
fn read_file(path: &Path) -> Result<toml::Table, Error> {
    let text = std::fs::read_to_string(path).map_err(|source| Error::ReadFile {
        path: path.to_owned(),
        source,
    })?;
    toml::from_str(&text).map_err(|error| Error::Syntax {
        path: path.to_owned(),
        message: error.to_string().trim_end().to_owned(),
    })
}

/// Checks every key and value of a table from the file against `fields`, adding a problem for
/// each one that is not a setting or does not fit it. `root` is the whole file, which decides
/// the names of map entries when suggesting a key.
fn check_table(
    table: &toml::Table,
    fields: &[Field],
    path: &mut Vec<String>,
    root: &toml::Table,
    file: &Path,
    problems: &mut Vec<Error>,
) {
    for (key, value) in table {
        path.push(key.clone());
        let place = format!("`{}` in {}", path.join("."), file.display());
        match fields.iter().find(|field| field.name == key) {
            None => {
                // Not the sections the key is in: the key is unknown inside them.
                let candidates: Vec<Vec<String>> = schema::key_paths(root, true)
                    .into_iter()
                    .filter(|candidate| !path.starts_with(candidate))
                    .collect();
                let nearest = schema::nearest(path, &candidates)
                    .map(|nearest| nearest.join("."))
                    .unwrap_or_default();
                problems.push(Error::UnknownKey {
                    key: path.join("."),
                    path: file.to_owned(),
                    nearest,
                });
            }
            Some(field) => match (&field.node, value) {
                (Node::Leaf(leaf), value) => {
                    if let Err(reason) = (leaf.check)(value) {
                        problems.push(Error::Value { place, reason });
                    }
                }
                (Node::Section(inner), toml::Value::Table(section)) => {
                    check_table(section, inner, path, root, file, problems);
                }
                (Node::Map { entry, .. }, toml::Value::Table(entries)) => {
                    for (name, value) in entries {
                        path.push(name.clone());
                        match value {
                            _ if !schema::is_entry_name(name) => problems.push(Error::Value {
                                place: format!("`{}` in {}", path.join("."), file.display()),
                                reason: "a name here is lowercase letters and digits, in words \
                                         joined by single underscores, at most 63 characters"
                                    .to_owned(),
                            }),
                            toml::Value::Table(section) => {
                                check_table(section, entry, path, root, file, problems);
                            }
                            _ => problems.push(Error::Value {
                                place: format!("`{}` in {}", path.join("."), file.display()),
                                reason: "expected a table".to_owned(),
                            }),
                        }
                        path.pop();
                    }
                }
                _ => problems.push(Error::Value {
                    place,
                    reason: "expected a table".to_owned(),
                }),
            },
        }
        path.pop();
    }
}

/// An unknown `OPENQTT_` variable, with the variable to set instead: the new name of one that
/// was renamed, or else the nearest valid one.
fn unknown_variable(name: &str, table: &toml::Table) -> Error {
    let renamed = RENAMED
        .iter()
        .find(|(old, _)| *old == name)
        .map(|(_, new)| (*new).to_owned());
    let nearest = renamed.unwrap_or_else(|| {
        let unknown: Vec<String> = name
            .strip_prefix(PREFIX)
            .unwrap_or(name)
            .split("__")
            .map(str::to_ascii_lowercase)
            .collect();
        schema::nearest(&unknown, &schema::key_paths(table, false))
            .map(|nearest| schema::variable(&nearest))
            .unwrap_or_default()
    });
    Error::UnknownVariable {
        name: name.to_owned(),
        nearest,
    }
}

/// An `OTEL_` variable, with the setting that does what it was meant to.
fn otel_variable(name: &str) -> Error {
    let key = [
        ("_HEADERS", "headers_file"),
        ("_TIMEOUT", "timeout"),
        ("_INTERVAL", "interval"),
        ("_CERTIFICATE", "ca_file"),
    ]
    .iter()
    .find(|(suffix, _)| name.ends_with(suffix))
    .map_or("endpoint", |(_, key)| key);
    Error::OtelVariable {
        name: name.to_owned(),
        setting: schema::variable(&["observability", "otlp", key]),
    }
}

/// Sets the value at `path`, making the tables above it as needed.
fn insert_at(table: &mut toml::Table, path: &[String], value: toml::Value) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut current = table;
    for segment in parents {
        let next = current
            .entry(segment.clone())
            .or_insert(toml::Value::Table(toml::Table::new()));
        let Some(next) = next.as_table_mut() else {
            return;
        };
        current = next;
    }
    current.insert(last.clone(), value);
}

/// Removes the value at `path`, so the setting falls back to its default.
fn remove_at(table: &mut toml::Table, path: &[String]) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut current = table;
    for segment in parents {
        let Some(next) = current.get_mut(segment).and_then(toml::Value::as_table_mut) else {
            return;
        };
        current = next;
    }
    current.remove(last);
}
