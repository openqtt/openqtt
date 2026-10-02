//! Writes the settings reference into docs/spec/config.md, between its markers. `make
//! config-doc` runs it after a setting changes; until it has, a test of this crate fails.

#[path = "../tests/support/doc.rs"]
mod doc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = doc::path();
    let document = std::fs::read_to_string(&path)?;
    let updated = doc::with_reference(&document, &openqtt_config::reference())
        .ok_or("docs/spec/config.md has lost its settings markers")?;
    if updated != document {
        std::fs::write(&path, updated)?;
    }
    Ok(())
}
