//! docs/spec/config.md documents every setting, in tables the declarations write: a setting
//! cannot be added, renamed or given a new default without the document following.

#[path = "support/doc.rs"]
mod doc;

#[test]
fn docs_spec_config_md_holds_the_reference_the_declarations_write() {
    let document = std::fs::read_to_string(doc::path()).unwrap();
    let expected = doc::with_reference(&document, &openqtt_config::reference())
        .expect("docs/spec/config.md has lost its settings markers");
    assert!(
        document == expected,
        "docs/spec/config.md is out of date with crates/config/src/settings.rs: run `make config-doc`"
    );
}
