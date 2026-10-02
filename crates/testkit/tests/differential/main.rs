//! The differential harness: the scenarios of `scenarios.rs` played against OpenQTT 1.x in
//! Docker, the oracle, and compared after normalizing. `README.md` beside this file says how
//! to run it and what it checks.
//!
//! Until OpenQTT 2.0 runs, it proves the harness itself: two runs against the oracle must
//! produce the same traces, and those must match the traces kept in `oracle/`. The Docker runs
//! are ignored tests, so `make check` does not need Docker; `make differential` runs them.
//! What does run in `make check` is the bookkeeping: every scenario and every divergence names
//! statements and decisions report R1 defines.

mod scenarios;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use openqtt_testkit::differential::{diff, parse_divergences, r1_decisions, r1_statements};
use openqtt_testkit::{ORACLE_IMAGE, Oracle, Outcome, Runner, Scenario};
use serde_json::Value;

/// Report R1, which every statement and decision a scenario names must come from.
const R1: &str = include_str!("../../../../docs/reports/R01-conformance.md");

/// The intended differences from OpenQTT 1.x, D1 to D32.
const DIVERGENCES: &str = include_str!("divergences.toml");

/// How many scenarios the starter catalogue holds at least.
const STARTER: usize = 15;

/// How long OpenQTT 1.x may take to start serving.
const STARTUP: Duration = Duration::from_secs(120);

/// The traces kept in the repository: what OpenQTT 1.x does, normalized.
fn kept_traces() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/differential/oracle")
}

/// Where a run writes its traces, normalized and raw.
fn run_traces(run: &str) -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("differential")
        .join(run)
}

fn pretty(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("a trace serializes");
    text.push('\n');
    text
}

/// Plays every scenario once, writes its traces under `run`, and returns the outcomes by name.
async fn play(runner: &Runner, catalogue: &[Scenario], run: &str) -> BTreeMap<String, Outcome> {
    let normalized = run_traces(run);
    let raw = normalized.join("raw");
    fs::create_dir_all(&raw).expect("the run's trace directory");
    let mut outcomes = BTreeMap::new();
    for scenario in catalogue {
        let outcome = runner.run(scenario).await;
        let file = format!("{}.json", scenario.name);
        fs::write(normalized.join(&file), pretty(&outcome.trace())).expect("a trace file");
        fs::write(raw.join(&file), pretty(&outcome.raw_trace())).expect("a raw trace file");
        outcomes.insert(scenario.name.clone(), outcome);
    }
    outcomes
}

/// Starts the oracle and waits until it serves.
async fn oracle() -> Oracle {
    let oracle = Oracle::start(ORACLE_IMAGE).unwrap_or_else(|error| panic!("{error}"));
    if let Err(error) = oracle.ready(STARTUP).await {
        panic!("{error}");
    }
    oracle
}

/// Plays the catalogue twice and returns every way the runs failed or differ.
async fn two_runs(oracle: &Oracle) -> (BTreeMap<String, Outcome>, Vec<String>) {
    let runner = Runner::new(oracle.target().clone());
    let catalogue = scenarios::catalogue();
    let first = play(&runner, &catalogue, "run-a").await;
    let second = play(&runner, &catalogue, "run-b").await;
    let mut problems = Vec::new();
    for (name, a) in &first {
        for outcome in [a, &second[name]] {
            if !outcome.failures.is_empty() {
                problems.push(format!("{name}: {:#?}", outcome.failures));
            }
        }
        if let Some(difference) = diff(&a.trace(), &second[name].trace()) {
            problems.push(format!("{name}: the two runs differ\n{difference}"));
        }
    }
    (first, problems)
}

#[tokio::test]
#[ignore = "needs Docker: make differential"]
async fn the_oracle_traces_the_same_twice_and_as_kept() {
    let oracle = oracle().await;
    let (first, mut problems) = two_runs(&oracle).await;
    for (name, outcome) in &first {
        let kept = kept_traces().join(format!("{name}.json"));
        match fs::read_to_string(&kept) {
            Ok(text) => {
                let trace: Value = serde_json::from_str(&text).expect("a kept trace parses");
                if let Some(difference) = diff(&trace, &outcome.trace()) {
                    problems.push(format!(
                        "{name}: differs from {}; if the change is intended, make \
                         differential-bless\n{difference}",
                        kept.display()
                    ));
                }
            }
            Err(_) => problems.push(format!(
                "{name}: no kept trace at {}; make differential-bless",
                kept.display()
            )),
        }
    }
    assert!(
        problems.is_empty(),
        "{}\n\ntraces: {}\noracle log:\n{}",
        problems.join("\n\n"),
        run_traces("").display(),
        oracle.logs()
    );
}

#[tokio::test]
#[ignore = "needs Docker: make differential-bless"]
async fn bless_the_oracle_traces() {
    let oracle = oracle().await;
    let (first, problems) = two_runs(&oracle).await;
    // Only traces that two runs agree on are worth keeping.
    assert!(problems.is_empty(), "{}", problems.join("\n\n"));
    let kept = kept_traces();
    fs::create_dir_all(&kept).expect("the kept trace directory");
    for (name, outcome) in &first {
        fs::write(kept.join(format!("{name}.json")), pretty(&outcome.trace()))
            .expect("a kept trace file");
    }
}

#[test]
fn the_catalogue_names_what_r1_defines() {
    let statements = r1_statements(R1);
    let decisions = r1_decisions(R1);
    let catalogue = scenarios::catalogue();
    assert!(
        catalogue.len() >= STARTER,
        "the starter catalogue has {} scenarios",
        catalogue.len()
    );
    let mut names = BTreeSet::new();
    for scenario in &catalogue {
        let name = &scenario.name;
        assert!(
            names.insert(name.clone()),
            "{name} is in the catalogue twice"
        );
        assert!(
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "{name} is not a file name"
        );
        assert!(!scenario.summary.is_empty(), "{name} has no summary");
        assert!(!scenario.statements.is_empty(), "{name} names no statement");
        for id in &scenario.statements {
            assert!(
                statements.contains(id),
                "{name} names {id}, which R1 does not list"
            );
        }
        for id in &scenario.divergences {
            assert!(
                decisions.contains(id),
                "{name} names {id}, which R1 does not define"
            );
        }
    }
}

#[test]
fn the_divergences_are_d1_to_d32_once_each_and_name_what_exists() {
    let divergences = parse_divergences(DIVERGENCES).unwrap();
    let decisions = r1_decisions(R1);
    let statements = r1_statements(R1);
    let catalogue = scenarios::catalogue();
    let scenarios: BTreeMap<&str, &Scenario> = catalogue
        .iter()
        .map(|scenario| (scenario.name.as_str(), scenario))
        .collect();

    let ids: Vec<&str> = divergences.iter().map(|d| d.id.as_str()).collect();
    let expected: Vec<String> = (1..=32).map(|n| format!("D{n}")).collect();
    assert_eq!(ids, expected, "divergences.toml lists D1 to D32 in order");
    assert_eq!(
        decisions,
        expected.iter().cloned().collect::<BTreeSet<_>>(),
        "R1 defines D1 to D32"
    );

    let em_dash = '\u{2014}';
    for divergence in &divergences {
        let id = &divergence.id;
        for text in [&divergence.summary, &divergence.emqx, &divergence.openqtt] {
            assert!(!text.trim().is_empty(), "{id} has an empty field");
            assert!(!text.contains(em_dash), "{id} has an em dash");
        }
        for statement in &divergence.statements {
            assert!(
                statements.contains(statement),
                "{id} names {statement}, which R1 does not list"
            );
        }
        for name in &divergence.scenarios {
            let scenario = scenarios
                .get(name.as_str())
                .unwrap_or_else(|| panic!("{id} names {name}, which is not in the catalogue"));
            assert!(
                scenario.divergences.contains(id),
                "{id} names {name}, which does not name {id} back"
            );
        }
    }
    for scenario in &catalogue {
        for id in &scenario.divergences {
            let listed = divergences
                .iter()
                .any(|d| &d.id == id && d.scenarios.contains(&scenario.name));
            assert!(
                listed,
                "{} names {id}, whose entry does not list it",
                scenario.name
            );
        }
    }
}
