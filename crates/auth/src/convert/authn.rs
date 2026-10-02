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
use crate::password::{DEFAULT_ITERATIONS, PasswordHash};

/// A converted user file.
#[derive(Clone, Debug)]
pub struct AuthnConversion {
    /// The 2.0 bootstrap file, `hashed`.
    pub text: String,
    /// How many users it holds.
    pub users: usize,
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
        if user.contains('\0') || password.contains('\0') {
            problems.push(format!("line {line}: a field contains U+0000"));
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
        let hash = PasswordHash::new(password.as_bytes())?;
        let _ = writeln!(text, "{user},{hash}");
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
    use super::*;
    use crate::password::{BootstrapFormat, Check, CredentialClass, PasswordList};

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
