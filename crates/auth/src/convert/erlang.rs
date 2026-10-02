//! Erlang terms, as `file:consult` reads a file of them: each term followed by a full stop.
//!
//! Written from the Erlang reference manual's grammar for the data types an `acl.conf` of
//! OpenQTT 1.x uses: atoms, plain and quoted; strings, with every escape sequence, and adjacent
//! strings joined; binaries of strings and bytes; decimal integers; tuples and lists. Anything
//! else, a variable, a map, a float, a fun, is refused with its line. Comments run from `%` to
//! the end of the line.

use std::fmt;

use crate::Error;

/// A term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Term {
    /// An atom, plain or quoted: `allow`, `'and'`.
    Atom(String),
    /// A string in double quotes, its escapes resolved.
    Str(String),
    /// A binary, `<<"text">>`, as its bytes.
    Bin(Vec<u8>),
    /// A decimal integer.
    Int(i64),
    /// `{...}`.
    Tuple(Vec<Term>),
    /// `[...]`.
    List(Vec<Term>),
}

/// A term of the file, and the line it starts on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Form {
    pub(crate) term: Term,
    pub(crate) line: usize,
}

/// Every term of `source`, in order.
pub(crate) fn parse(source: &str) -> Result<Vec<Form>, Error> {
    let mut reader = Reader {
        chars: source.chars().collect(),
        at: 0,
        line: 1,
    };
    let mut forms = Vec::new();
    loop {
        reader.skip_blank();
        if reader.peek().is_none() {
            return Ok(forms);
        }
        let line = reader.line;
        let term = reader.term()?;
        reader.skip_blank();
        if reader.peek() != Some('.') {
            return Err(reader.fail("a term ends with a full stop"));
        }
        reader.at += 1;
        if reader
            .peek()
            .is_some_and(|c| !c.is_whitespace() && c != '%')
        {
            return Err(reader.fail("a full stop is followed by white space"));
        }
        forms.push(Form { term, line });
    }
}

struct Reader {
    chars: Vec<char>,
    at: usize,
    line: usize,
}

impl Reader {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.at).copied()
    }

    fn next(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.at += 1;
        if c == '\n' {
            self.line += 1;
        }
        Some(c)
    }

    fn fail(&self, reason: &str) -> Error {
        Error::Convert {
            line: self.line,
            reason: reason.to_owned(),
        }
    }

    /// White space and comments.
    fn skip_blank(&mut self) {
        while let Some(c) = self.peek() {
            if c == '%' {
                while self.peek().is_some_and(|c| c != '\n') {
                    self.next();
                }
            } else if c.is_whitespace() {
                self.next();
            } else {
                break;
            }
        }
    }

    fn term(&mut self) -> Result<Term, Error> {
        self.skip_blank();
        match self.peek() {
            Some('{') => {
                self.next();
                Ok(Term::Tuple(self.sequence('}')?))
            }
            Some('[') => {
                self.next();
                Ok(Term::List(self.sequence(']')?))
            }
            Some('"') => {
                let mut text = self.quoted('"')?;
                // Adjacent strings are one string.
                loop {
                    self.skip_blank();
                    if self.peek() != Some('"') {
                        break;
                    }
                    text.push_str(&self.quoted('"')?);
                }
                Ok(Term::Str(text))
            }
            Some('\'') => Ok(Term::Atom(self.quoted('\'')?)),
            Some('<') => self.binary(),
            Some(c) if c.is_ascii_digit() || c == '-' => self.integer(),
            Some(c) if c.is_lowercase() => {
                let mut name = String::new();
                while let Some(c) = self
                    .peek()
                    .filter(|&c| c.is_alphanumeric() || c == '_' || c == '@')
                {
                    name.push(c);
                    self.next();
                }
                Ok(Term::Atom(name))
            }
            Some(c) if c.is_uppercase() || c == '_' => {
                Err(self.fail("a variable is not a term this file can hold"))
            }
            Some('#') => Err(self.fail("a map is not a term this file can hold")),
            Some(_) => Err(self.fail("not the start of a term")),
            None => Err(self.fail("the file ends inside a term")),
        }
    }

    /// Terms separated by commas, up to `close`.
    fn sequence(&mut self, close: char) -> Result<Vec<Term>, Error> {
        let mut items = Vec::new();
        self.skip_blank();
        if self.peek() == Some(close) {
            self.next();
            return Ok(items);
        }
        loop {
            items.push(self.term()?);
            self.skip_blank();
            match self.next() {
                Some(',') => {}
                Some(c) if c == close => return Ok(items),
                Some('|') => {
                    return Err(self.fail("an improper list is not a term this file can hold"));
                }
                _ => {
                    return Err(self.fail(if close == '}' {
                        "a tuple's items are separated by commas and end with `}`"
                    } else {
                        "a list's items are separated by commas and end with `]`"
                    }));
                }
            }
        }
    }

    fn integer(&mut self) -> Result<Term, Error> {
        let mut text = String::new();
        if self.peek() == Some('-') {
            text.push('-');
            self.next();
        }
        while let Some(c) = self.peek().filter(char::is_ascii_digit) {
            text.push(c);
            self.next();
        }
        if matches!(self.peek(), Some('#' | '.' | 'e' | 'E' | '_'))
            && self
                .chars
                .get(self.at + 1)
                .is_some_and(char::is_ascii_alphanumeric)
        {
            return Err(self.fail("only decimal integers are terms this file can hold"));
        }
        text.parse()
            .map(Term::Int)
            .map_err(|_| self.fail("not a decimal integer"))
    }

    /// `<<>>`, `<<"text">>`, `<<"text"/utf8>>`, `<<1, 2>>`, and segments of them.
    fn binary(&mut self) -> Result<Term, Error> {
        self.next();
        if self.next() != Some('<') {
            return Err(self.fail("a binary begins with `<<`"));
        }
        let mut bytes = Vec::new();
        self.skip_blank();
        if self.peek() == Some('>') {
            self.next();
            return self.close_binary(bytes);
        }
        loop {
            self.skip_blank();
            match self.term()? {
                Term::Str(text) => {
                    self.skip_blank();
                    let utf8 = if self.peek() == Some('/') {
                        self.next();
                        let mut kind = String::new();
                        while let Some(c) = self.peek().filter(char::is_ascii_alphanumeric) {
                            kind.push(c);
                            self.next();
                        }
                        if kind != "utf8" {
                            return Err(self.fail("a binary's string segment is plain or /utf8"));
                        }
                        true
                    } else {
                        false
                    };
                    if utf8 {
                        bytes.extend_from_slice(text.as_bytes());
                    } else {
                        for c in text.chars() {
                            let byte = u8::try_from(u32::from(c)).map_err(|_| {
                                self.fail("a character above 255 in a binary needs /utf8")
                            })?;
                            bytes.push(byte);
                        }
                    }
                }
                Term::Int(value) => bytes.push(
                    u8::try_from(value).map_err(|_| self.fail("a byte of a binary is 0 to 255"))?,
                ),
                _ => return Err(self.fail("a binary holds strings and bytes")),
            }
            self.skip_blank();
            match self.next() {
                Some(',') => {}
                Some('>') => return self.close_binary(bytes),
                _ => return Err(self.fail("a binary's segments are separated by commas")),
            }
        }
    }

    fn close_binary(&mut self, bytes: Vec<u8>) -> Result<Term, Error> {
        if self.next() == Some('>') {
            Ok(Term::Bin(bytes))
        } else {
            Err(self.fail("a binary ends with `>>`"))
        }
    }

    /// A string or a quoted atom, its escapes resolved.
    fn quoted(&mut self, quote: char) -> Result<String, Error> {
        self.next();
        let mut text = String::new();
        loop {
            match self.next() {
                None => return Err(self.fail("the file ends inside quotes")),
                Some(c) if c == quote => return Ok(text),
                Some('\\') => text.push(self.escape()?),
                Some(c) => text.push(c),
            }
        }
    }

    fn escape(&mut self) -> Result<char, Error> {
        let fail = |reader: &Self| reader.fail("an escape sequence names no character");
        let c = self.next().ok_or_else(|| fail(self))?;
        let code = match c {
            'b' => 8,
            'd' => 127,
            'e' => 27,
            'f' => 12,
            'n' => 10,
            'r' => 13,
            's' => 32,
            't' => 9,
            'v' => 11,
            '0'..='7' => {
                let mut value = c.to_digit(8).unwrap_or(0);
                for _ in 0..2 {
                    match self.peek().and_then(|c| c.to_digit(8)) {
                        Some(digit) => {
                            value = value * 8 + digit;
                            self.next();
                        }
                        None => break,
                    }
                }
                value
            }
            'x' => {
                let mut digits = String::new();
                if self.peek() == Some('{') {
                    self.next();
                    while let Some(c) = self.next() {
                        if c == '}' {
                            break;
                        }
                        digits.push(c);
                    }
                } else {
                    for _ in 0..2 {
                        if let Some(c) = self.next() {
                            digits.push(c);
                        }
                    }
                }
                u32::from_str_radix(&digits, 16).map_err(|_| fail(self))?
            }
            '^' => {
                let control = self.next().ok_or_else(|| fail(self))?;
                if !control.is_ascii_alphabetic() {
                    return Err(fail(self));
                }
                u32::from(control.to_ascii_lowercase()) - u32::from('a') + 1
            }
            // `\"`, `\'`, `\\`, and any other character stand for themselves.
            other => u32::from(other),
        };
        char::from_u32(code).ok_or_else(|| fail(self))
    }
}

impl fmt::Display for Term {
    /// The term as Erlang writes it, for the comment above a converted rule.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Atom(name) => {
                let plain = name.chars().next().is_some_and(char::is_lowercase)
                    && name
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '_' || c == '@')
                    && !matches!(name.as_str(), "and" | "or" | "not" | "xor");
                if plain {
                    f.write_str(name)
                } else {
                    write_quoted(f, name, '\'')
                }
            }
            Self::Str(text) => write_quoted(f, text, '"'),
            Self::Bin(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) if text.is_ascii() => {
                    f.write_str("<<")?;
                    write_quoted(f, text, '"')?;
                    f.write_str(">>")
                }
                Ok(text) => {
                    f.write_str("<<")?;
                    write_quoted(f, text, '"')?;
                    f.write_str("/utf8>>")
                }
                Err(_) => {
                    let bytes: Vec<String> = bytes.iter().map(u8::to_string).collect();
                    write!(f, "<<{}>>", bytes.join(","))
                }
            },
            Self::Int(value) => write!(f, "{value}"),
            Self::Tuple(items) => write_items(f, items, '{', '}'),
            Self::List(items) => write_items(f, items, '[', ']'),
        }
    }
}

fn write_items(f: &mut fmt::Formatter<'_>, items: &[Term], open: char, close: char) -> fmt::Result {
    write!(f, "{open}")?;
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    write!(f, "{close}")
}

fn write_quoted(f: &mut fmt::Formatter<'_>, text: &str, quote: char) -> fmt::Result {
    write!(f, "{quote}")?;
    for c in text.chars() {
        match c {
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\t' => f.write_str("\\t")?,
            c if c == quote => write!(f, "\\{c}")?,
            c if c.is_control() => write!(f, "\\x{{{:X}}}", u32::from(c))?,
            c => write!(f, "{c}")?,
        }
    }
    write!(f, "{quote}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(source: &str) -> Term {
        let mut forms = parse(source).unwrap();
        assert_eq!(forms.len(), 1, "{source}");
        forms.remove(0).term
    }

    fn atom(name: &str) -> Term {
        Term::Atom(name.into())
    }

    #[test]
    fn rules_read_as_terms() {
        let forms = parse(
            "%% comment\n{allow, {username, {re, \"^dashboard$\"}}, subscribe, [\"$SYS/#\"]}.\n\
             \n{'and', [{ipaddr, \"10.0.0.0/8\"}]}. % trailing\n{deny, all}.",
        )
        .unwrap();
        assert_eq!(forms.len(), 3);
        assert_eq!(forms[0].line, 2);
        assert_eq!(forms[1].line, 4);
        assert_eq!(forms[2].line, 5);
        assert_eq!(
            forms[0].term,
            Term::Tuple(vec![
                atom("allow"),
                Term::Tuple(vec![
                    atom("username"),
                    Term::Tuple(vec![atom("re"), Term::Str("^dashboard$".into())]),
                ]),
                atom("subscribe"),
                Term::List(vec![Term::Str("$SYS/#".into())]),
            ])
        );
        assert_eq!(
            forms[0].term.to_string(),
            "{allow, {username, {re, \"^dashboard$\"}}, subscribe, [\"$SYS/#\"]}"
        );
        assert_eq!(
            forms[1].term.to_string(),
            "{'and', [{ipaddr, \"10.0.0.0/8\"}]}"
        );
    }

    #[test]
    fn strings_resolve_every_escape() {
        assert_eq!(
            one(r#""a\"b\\c\n\t\s\x41\x{1F600}\101\^A\d\q"."#),
            Term::Str("a\"b\\c\n\t A\u{1F600}A\u{1}\u{7f}q".into())
        );
        // Adjacent strings are one.
        assert_eq!(one("\"ab\" \"cd\"."), Term::Str("abcd".into()));
        assert_eq!(one("'quoted atom'."), atom("quoted atom"));
        assert_eq!(one("node@host."), atom("node@host"));
    }

    #[test]
    fn binaries_and_integers_read() {
        assert_eq!(one("<<\"t/#\">>."), Term::Bin(b"t/#".to_vec()));
        assert_eq!(one("<<>>."), Term::Bin(Vec::new()));
        assert_eq!(
            one("<<\"caf\u{e9}\"/utf8>>."),
            Term::Bin("caf\u{e9}".as_bytes().to_vec())
        );
        assert_eq!(one("<<\"a\", 98>>."), Term::Bin(b"ab".to_vec()));
        assert_eq!(
            one("[0, 1, -2]."),
            Term::List(vec![Term::Int(0), Term::Int(1), Term::Int(-2)])
        );
        assert_eq!(one("{}."), Term::Tuple(Vec::new()));
        assert_eq!(one("[]."), Term::List(Vec::new()));
    }

    #[test]
    fn what_is_not_a_term_names_its_line() {
        let cases = [
            ("{allow, all}", 1, "a term ends with a full stop"),
            (
                "{allow, all}.x.",
                1,
                "a full stop is followed by white space",
            ),
            ("\n{allow, X}.", 2, "a variable"),
            ("#{a => b}.", 1, "a map"),
            ("[a | b].", 1, "an improper list"),
            ("{a b}.", 1, "a tuple's items"),
            ("\"open.", 1, "the file ends inside quotes"),
            ("16#ff.", 1, "only decimal integers"),
            ("1.5.", 1, "only decimal integers"),
            ("<<\"\u{e9}t\u{e9}\u{1F600}\">>.", 1, "needs /utf8"),
            ("<<256>>.", 1, "0 to 255"),
            ("{a, ", 1, "the file ends inside a term"),
        ];
        for (source, line, reason) in cases {
            let error = parse(source).unwrap_err();
            let Error::Convert { line: at, .. } = &error else {
                panic!("{error:?}");
            };
            assert_eq!(*at, line, "{source}");
            assert!(error.to_string().contains(reason), "{source}: {error}");
        }
    }
}
