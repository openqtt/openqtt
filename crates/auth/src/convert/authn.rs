//! `openqtt convert authn`: the bootstrap CSV of OpenQTT 1.x's built-in user database, with
//! plain passwords, into a 2.0 bootstrap file in the `hashed` format.
//!
//! The CSV is read as 1.x reads it ([emqx_utils_stream.erl L371-L430][csv]): a header row naming
//! the columns, then one user a row; rows are split on carriage returns and line feeds, blank
//! ones dropped, and fields on commas and spaces, empty ones dropped. So no field holds a comma
//! or a space, as no 1.x user could. The columns are `user_id`, `password` and, optionally,
//! `is_superuser`.
//!
//! A superuser skips every authorization rule in 1.x. 2.0 has no such user, so a file that
//! names one is refused: give that user ACL rules instead.
//!
//! [csv]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_utils/src/emqx_utils_stream.erl#L371-L430

use std::collections::HashSet;
use std::fmt::Write as _;

use super::acl::Note;
use crate::Error;
use crate::password::{DEFAULT_ITERATIONS, PasswordHash, bootstrap_name_problem, shown};

/// A converted user file.
#[derive(Clone, Debug)]
pub struct AuthnConversion {
    /// The 2.0 bootstrap file, `hashed`.
    pub text: String,
    /// How many users it holds.
    pub users: usize,
    /// Their names, in the order of the file.
    pub names: Vec<String>,
    /// What the conversion read differently from what the file may have meant, by line.
    pub warnings: Vec<Note>,
}

/// Converts `source`, hashing every password: each costs one PBKDF2 hash of
/// [`DEFAULT_ITERATIONS`].
///
/// # Errors
///
/// [`Error::Conversion`] with every problem: a header without `user_id` and `password`, a
/// column 1.x does not know, a row with the wrong number of fields, a user named twice, or a
/// superuser. [`Error::Random`] when no salt can be drawn.
pub fn convert_authn(source: &str) -> Result<AuthnConversion, Error> {
    convert_authn_with(source, &PasswordHash::new)
}

/// [`convert_authn`], hashing each password with `hash`, so that a test need not pay for the
/// real cost of a hash.
pub(crate) fn convert_authn_with(
    source: &str,
    hash: &dyn Fn(&[u8]) -> Result<PasswordHash, Error>,
) -> Result<AuthnConversion, Error> {
    // Lines are counted by line feeds; a carriage return separates rows within one.
    let mut rows = source
        .split('\n')
        .enumerate()
        .flat_map(|(index, line)| line.split('\r').map(move |row| (index + 1, row)))
        .filter(|(_, row)| !row.is_empty());
    let mut problems = Vec::new();
    let Some((header_line, header)) = rows.next() else {
        return Err(Error::Conversion {
            problems: vec!["the file is empty: it has no header row".to_owned()],
        });
    };
    let columns = fields(header);
    let position = |name: &str| columns.iter().position(|column| *column == name);
    let user_column = position("user_id").or_else(|| position("user"));
    let password_column = position("password");
    let superuser_column = position("is_superuser");
    for column in &columns {
        if !matches!(*column, "user_id" | "user" | "password" | "is_superuser") {
            let hint = if matches!(*column, "password_hash" | "salt") {
                ": only plain passwords can be converted, since 2.0 hashes differently"
            } else {
                ""
            };
            problems.push(format!(
                "line {header_line}: the column `{column}` is not one this converter reads{hint}"
            ));
        }
    }
    let (Some(user_column), Some(password_column)) = (user_column, password_column) else {
        problems.push(format!(
            "line {header_line}: the header names the columns, and must have `user_id` and \
             `password`"
        ));
        return Err(Error::Conversion { problems });
    };
    let mut text = format!(
        "# Converted from a user file of OpenQTT 1.x by `openqtt convert authn`: the `hashed`\n\
         # format, pbkdf2-sha256 at {DEFAULT_ITERATIONS} iterations.\n"
    );
    let mut seen = HashSet::new();
    let mut warnings = Vec::new();
    let mut superusers = Vec::new();
    let mut names = Vec::new();
    let mut users = 0;
    for (line, row) in rows {
        let values = fields(row);
        if values.len() != columns.len() {
            problems.push(format!(
                "line {line}: {} fields where the header names {} columns; a field cannot hold \
                 a comma or a space",
                values.len(),
                columns.len()
            ));
            continue;
        }
        let user = values[user_column];
        let password = values[password_column];
        // A name the bootstrap file would read back differently, or not at all, is refused
        // here rather than written and lost.
        if let Some(problem) = bootstrap_name_problem(user) {
            problems.push(format!(
                "line {line}: the user name {} {problem}",
                shown(user)
            ));
            continue;
        }
        if password.contains('\0') {
            problems.push(format!("line {line}: the password contains U+0000"));
            continue;
        }
        if !seen.insert(user) {
            problems.push(format!("line {line}: the user `{user}` is named before"));
            continue;
        }
        match superuser_column.map(|column| values[column]) {
            Some("true") => {
                superusers.push(format!("`{user}` (line {line})"));
                continue;
            }
            None | Some("false") => {}
            Some(other) => warnings.push(Note {
                line,
                message: format!(
                    "is_superuser is `{other}`, which 1.x read as false, and so does this"
                ),
            }),
        }
        let hash = hash(password.as_bytes())?;
        let _ = writeln!(text, "{user},{hash}");
        names.push(user.to_owned());
        users += 1;
    }
    if !superusers.is_empty() {
        problems.push(format!(
            "superusers skip every authorization rule in 1.x, and 2.0 has none: {}; give each \
             ACL rules for what it needs, and mark it is_superuser false",
            superusers.join(", ")
        ));
    }
    if !problems.is_empty() {
        return Err(Error::Conversion { problems });
    }
    Ok(AuthnConversion {
        text,
        users,
        names,
        warnings,
    })
}

/// The fields of a row: split on commas and spaces, the empty ones dropped.
fn fields(row: &str) -> Vec<&str> {
    row.split([',', ' '])
        .filter(|field| !field.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use proptest::prelude::*;

    use super::*;
    use crate::password::{BootstrapFormat, Check, CredentialClass, MIN_ITERATIONS, PasswordList};

    fn problems(source: &str) -> Vec<String> {
        match convert_authn(source) {
            Err(Error::Conversion { problems }) => problems,
            other => panic!("not refused: {other:?}"),
        }
    }

    #[test]
    fn users_become_hashed_lines_that_load() {
        let converted = convert_authn(
            "user_id,password,is_superuser\r\nalice,wonder,false\r\nbob,builder,false\r\n",
        )
        .unwrap();
        assert_eq!(converted.users, 2);
        assert!(converted.warnings.is_empty());
        let list = PasswordList::parse(&converted.text, BootstrapFormat::Hashed, None).unwrap();
        assert_eq!(
            list.check("alice", b"wonder"),
            Check::Valid(CredentialClass::User)
        );
        assert_eq!(
            list.check("bob", b"builder"),
            Check::Valid(CredentialClass::User)
        );
        assert!(!converted.text.contains("wonder"), "{}", converted.text);
        assert!(converted.text.starts_with("# Converted from a user file"));
    }

    #[test]
    fn a_superuser_is_refused() {
        let found = problems(
            "user_id,password,is_superuser\nroot,s3cret-root,true\nalice,y,false\nadmin,s3cret-admin,true\n",
        );
        assert_eq!(found.len(), 1);
        assert!(
            found[0].contains("`root` (line 2), `admin` (line 4)"),
            "{found:?}"
        );
        assert!(!found[0].contains("s3cret"), "{found:?}");
    }

    #[test]
    fn rows_are_read_as_1x_read_them() {
        // Spaces separate fields too, and blank lines are skipped.
        let converted = convert_authn("user_id password\n\n  carol   pa55  \n").unwrap();
        assert_eq!(converted.users, 1);
        let list = PasswordList::parse(&converted.text, BootstrapFormat::Hashed, None).unwrap();
        assert_eq!(
            list.check("carol", b"pa55"),
            Check::Valid(CredentialClass::User)
        );
        // Anything but `true` was not a superuser in 1.x.
        let converted = convert_authn("user,password,is_superuser\ndave,p,TRUE\n").unwrap();
        assert_eq!(converted.warnings.len(), 1);
        assert_eq!(converted.warnings[0].line, 2);
    }

    /// One hash for every user of the property below: it is about names, and a real hash per
    /// user would take most of a second in a debug build.
    fn fixed_hash() -> &'static PasswordHash {
        static HASH: OnceLock<PasswordHash> = OnceLock::new();
        HASH.get_or_init(|| PasswordHash::with_iterations(b"pw", MIN_ITERATIONS).unwrap())
    }

    /// A name, mostly ordinary, sometimes with what a bootstrap file reads specially: `#`, a
    /// comma, white space of several kinds, line breaks, U+0000.
    fn name() -> impl Strategy<Value = String> {
        let ordinary =
            prop::sample::select(&['a', 'b', 'Z', '7', '-', '_', ':', '/', '.', '\u{e9}'][..]);
        let special = prop::sample::select(
            &['#', ',', ' ', '\t', '\u{a0}', '\u{3000}', '\r', '\n', '\0'][..],
        );
        prop::collection::vec(prop_oneof![9 => ordinary, 1 => special], 0..10)
            .prop_map(|chars| chars.into_iter().collect())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]

        #[test]
        fn every_name_the_converter_takes_reads_back_the_same(
            names in prop::collection::vec(name(), 1..5),
        ) {
            let mut source = String::from("user_id,password\n");
            for name in &names {
                source.push_str(name);
                source.push_str(",pw\n");
            }
            if let Ok(converted) = convert_authn_with(&source, &|_| Ok(fixed_hash().clone())) {
                let list = PasswordList::parse(&converted.text, BootstrapFormat::Hashed, None);
                prop_assert!(list.is_ok(), "{:?}\n{}", list, converted.text);
                let list = list.unwrap();
                let mut read: Vec<&str> = list.names().collect();
                read.sort_unstable();
                let mut written: Vec<&str> = converted.names.iter().map(String::as_str).collect();
                written.sort_unstable();
                prop_assert_eq!(read, written, "{}", converted.text);
            }
        }
    }

    /// The property means something only if many of the files drawn convert.
    #[test]
    fn the_files_drawn_often_convert() {
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::deterministic();
        let strategy = prop::collection::vec(name(), 1..5);
        let mut converted = 0;
        for _ in 0..500 {
            let names = strategy.new_tree(&mut runner).unwrap().current();
            let mut source = String::from("user_id,password\n");
            for name in &names {
                source.push_str(name);
                source.push_str(",pw\n");
            }
            if convert_authn_with(&source, &|_| Ok(fixed_hash().clone())).is_ok() {
                converted += 1;
            }
        }
        assert!(converted >= 50, "{converted} of 500 converted");
    }

    #[test]
    fn names_a_bootstrap_file_cannot_hold_are_refused_by_line() {
        let long = "n".repeat(65_536);
        for name in [
            "#alice",
            "\talice",
            "alice\u{a0}",
            "\u{3000}",
            long.as_str(),
        ] {
            let found = problems(&format!("user_id,password\nbob,pw\n{name},pw\n"));
            assert_eq!(found.len(), 1, "{name:?}: {found:?}");
            assert!(found[0].starts_with("line 3: the user name "), "{found:?}");
        }
    }

    #[test]
    fn what_cannot_be_converted_is_named_by_line() {
        assert_eq!(
            problems("user_id,password\nalice,a b\n"),
            [
                "line 2: 3 fields where the header names 2 columns; a field cannot hold a comma or a space"
            ]
        );
        assert_eq!(
            problems("user_id,password\nalice,a\nalice,b\n"),
            ["line 3: the user `alice` is named before"]
        );
        let hashed = problems("user_id,password_hash,salt,is_superuser\nalice,abc,def,false\n");
        assert!(
            hashed[0].contains("only plain passwords can be converted"),
            "{hashed:?}"
        );
        assert!(
            hashed
                .last()
                .unwrap()
                .contains("must have `user_id` and `password`")
        );
        assert_eq!(problems(""), ["the file is empty: it has no header row"]);
    }
}
