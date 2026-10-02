//! Liveness and readiness, as the HTTP listener of every process answers them.
//!
//! `/healthz` says the process is alive and asks nothing else: a probe that restarts a container
//! must never fail because something the process depends on did (R2 rule 29). `/readyz` says
//! whether the process can serve now, from the checks its components register in a
//! [`Registry`]: 200 while no check reports an error and 503 once one does, with every check in
//! the body either way, since a status code cannot say which component is down. Kubernetes reads
//! the status code alone.
//!
//! Serving them over HTTP belongs to the admin crate; this module decides what they answer.

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;
use std::sync::{PoisonError, RwLock};

use crate::Error;

/// What a readiness check reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The component can serve.
    Ready,
    /// The configuration leaves the component out, so it has nothing to be ready for. That does
    /// not make the process unready.
    Unconfigured,
    /// The component cannot serve, which makes the process unready.
    Error,
}

impl Status {
    /// The status as the body of `/readyz` writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Unconfigured => "unconfigured",
            Self::Error => "error",
        }
    }
}

/// One check's answer: a status and, unless it is ready, the reason.
///
/// A reason is a fixed phrase, `&'static str`, so an error's text, an address or a secret
/// cannot reach a body that `/readyz` gives anyone who can reach the port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readiness {
    status: Status,
    reason: Option<&'static str>,
}

impl Readiness {
    /// Ready to serve.
    pub const fn ready() -> Self {
        Self {
            status: Status::Ready,
            reason: None,
        }
    }

    /// Left out by the configuration, for `reason`.
    pub const fn unconfigured(reason: &'static str) -> Self {
        Self {
            status: Status::Unconfigured,
            reason: Some(reason),
        }
    }

    /// Unable to serve, for `reason`.
    pub const fn error(reason: &'static str) -> Self {
        Self {
            status: Status::Error,
            reason: Some(reason),
        }
    }

    /// The status.
    pub const fn status(self) -> Status {
        self.status
    }

    /// The reason, unless the check is ready.
    pub const fn reason(self) -> Option<&'static str> {
        self.reason
    }
}

/// What a probe answers: an HTTP status code and a JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// The status code: 200, or 503 for a process that is not ready.
    pub status: u16,
    /// The body, one JSON object.
    pub body: String,
}

/// The answer of `/healthz`: always 200, since a process that can answer is alive.
pub fn healthz() -> Probe {
    Probe {
        status: 200,
        body: r#"{"status":"ok"}"#.to_owned(),
    }
}

/// A check: called on every `/readyz`.
type Check = Box<dyn Fn() -> Readiness + Send + Sync>;

/// The readiness checks of a process's components, by name.
///
/// A check answers from state its component already holds, such as whether it has a leader or
/// a route view. It must not wait on the network or take long: a probe that times out reads as
/// a failure. Nor may it call the registry, whose lock it runs under.
#[derive(Default)]
pub struct Registry {
    checks: RwLock<BTreeMap<String, Check>>,
}

impl Registry {
    /// A registry without checks, which reads as ready.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `check` as `name`. A component registers before the HTTP listener starts, so a
    /// probe never misses it.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidCheckName`] unless the name is 1 to 64 lowercase letters, digits, `_`,
    /// `-` and `.`, and [`Error::DuplicateCheck`] when a check already has it.
    pub fn register<F>(&self, name: &str, check: F) -> Result<(), Error>
    where
        F: Fn() -> Readiness + Send + Sync + 'static,
    {
        let valid = (1..=64).contains(&name.len())
            && name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.".contains(&byte)
            });
        if !valid {
            return Err(Error::InvalidCheckName {
                name: name.to_owned(),
            });
        }
        let mut checks = self.checks.write().unwrap_or_else(PoisonError::into_inner);
        if checks.contains_key(name) {
            return Err(Error::DuplicateCheck {
                name: name.to_owned(),
            });
        }
        checks.insert(name.to_owned(), Box::new(check));
        Ok(())
    }

    /// The answer of `/readyz`: every check, asked now, and 503 if any reports an error.
    ///
    /// ```text
    /// {"status":"not_ready","checks":{"log":{"status":"error","reason":"no leader"},"otlp":{"status":"unconfigured","reason":"no endpoint"}}}
    /// ```
    pub fn readyz(&self) -> Probe {
        let checks = self.checks.read().unwrap_or_else(PoisonError::into_inner);
        let answers: Vec<(&str, Readiness)> = checks
            .iter()
            .map(|(name, check)| (name.as_str(), check()))
            .collect();
        let ready = answers
            .iter()
            .all(|(_, readiness)| readiness.status != Status::Error);

        let mut body = String::new();
        let _ = write!(
            body,
            r#"{{"status":"{}","checks":{{"#,
            if ready { "ready" } else { "not_ready" }
        );
        for (index, (name, readiness)) in answers.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            // A name is plain by construction; see `register`.
            let _ = write!(
                body,
                r#""{name}":{{"status":"{}""#,
                readiness.status.as_str()
            );
            if let Some(reason) = readiness.reason {
                let _ = write!(body, r#","reason":{}"#, JsonString(reason));
            }
            body.push('}');
        }
        body.push_str("}}");
        Probe {
            status: if ready { 200 } else { 503 },
            body,
        }
    }
}

/// Names only: a check is a closure.
impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let checks = self.checks.read().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("Registry")
            .field("checks", &checks.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A string as a JSON string literal.
struct JsonString<'a>(&'a str);

impl fmt::Display for JsonString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for c in self.0.chars() {
            match c {
                '"' => f.write_str("\\\"")?,
                '\\' => f.write_str("\\\\")?,
                '\n' => f.write_str("\\n")?,
                '\r' => f.write_str("\\r")?,
                '\t' => f.write_str("\\t")?,
                c if u32::from(c) < 0x20 => write!(f, "\\u{:04x}", u32::from(c))?,
                c => f.write_char(c)?,
            }
        }
        f.write_char('"')
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn healthz_is_always_ok() {
        assert_eq!(
            healthz(),
            Probe {
                status: 200,
                body: r#"{"status":"ok"}"#.to_owned()
            }
        );
    }

    #[test]
    fn a_registry_without_checks_is_ready() {
        let probe = Registry::new().readyz();
        assert_eq!(probe.status, 200);
        assert_eq!(probe.body, r#"{"status":"ready","checks":{}}"#);
    }

    #[test]
    fn unconfigured_checks_leave_the_process_ready_and_are_listed_by_name() {
        let registry = Registry::new();
        registry
            .register("otlp", || Readiness::unconfigured("no endpoint"))
            .unwrap();
        registry.register("log", Readiness::ready).unwrap();
        let probe = registry.readyz();
        assert_eq!(probe.status, 200);
        assert_eq!(
            probe.body,
            r#"{"status":"ready","checks":{"log":{"status":"ready"},"otlp":{"status":"unconfigured","reason":"no endpoint"}}}"#
        );
    }

    #[test]
    fn one_error_makes_the_process_unready_and_every_check_is_asked_each_time() {
        let registry = Registry::new();
        let leader = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&leader);
        registry
            .register("log", move || {
                if seen.load(Ordering::Relaxed) {
                    Readiness::ready()
                } else {
                    Readiness::error("no leader")
                }
            })
            .unwrap();
        registry.register("router.view", Readiness::ready).unwrap();

        let probe = registry.readyz();
        assert_eq!(probe.status, 503);
        assert_eq!(
            probe.body,
            r#"{"status":"not_ready","checks":{"log":{"status":"error","reason":"no leader"},"router.view":{"status":"ready"}}}"#
        );

        leader.store(true, Ordering::Relaxed);
        assert_eq!(registry.readyz().status, 200);
    }

    #[test]
    fn a_name_is_registered_once_and_must_be_plain() {
        let registry = Registry::new();
        registry.register("log", Readiness::ready).unwrap();
        let error = registry.register("log", Readiness::ready).unwrap_err();
        assert!(matches!(error, Error::DuplicateCheck { .. }), "{error}");
        for name in ["", "Log", "log partitions", "log\"", "é", &"a".repeat(65)] {
            let error = registry.register(name, Readiness::ready).unwrap_err();
            assert!(
                matches!(error, Error::InvalidCheckName { .. }),
                "{name}: {error}"
            );
        }
        assert_eq!(format!("{registry:?}"), r#"Registry { checks: ["log"] }"#);
    }

    #[test]
    fn the_body_is_json_whatever_a_reason_holds() {
        let registry = Registry::new();
        registry
            .register("odd", || {
                Readiness::error("quote \" backslash \\ line\nend \u{1} tab\t")
            })
            .unwrap();
        let body: serde_json::Value = serde_json::from_str(&registry.readyz().body).unwrap();
        assert_eq!(body["status"], "not_ready");
        assert_eq!(
            body["checks"]["odd"]["reason"],
            "quote \" backslash \\ line\nend \u{1} tab\t"
        );
        let health: serde_json::Value = serde_json::from_str(&healthz().body).unwrap();
        assert_eq!(health["status"], "ok");
    }
}
