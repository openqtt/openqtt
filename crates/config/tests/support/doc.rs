//! Where the settings reference sits in docs/spec/config.md, shared by the test that holds the
//! document to the declarations and the example that rewrites it.

use std::path::PathBuf;

/// The line that opens the generated reference.
pub const BEGIN: &str =
    "<!-- BEGIN SETTINGS: written by `make config-doc` from crates/config/src/settings.rs -->";

/// The line that closes it.
pub const END: &str = "<!-- END SETTINGS -->";

/// docs/spec/config.md.
pub fn path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/spec/config.md")
}

/// `document` with the text between its markers replaced by `reference`, or `None` when it has
/// lost either marker.
pub fn with_reference(document: &str, reference: &str) -> Option<String> {
    let (head, rest) = document.split_once(BEGIN)?;
    let (_, tail) = rest.split_once(END)?;
    Some(format!("{head}{BEGIN}\n\n{reference}\n{END}{tail}"))
}
