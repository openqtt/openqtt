//! The ACL: ordered rules in a TOML file, the first that matches deciding and no match denying
//! (report R2, rules 9 to 16). `docs/spec/acl.md` is the format.
//!
//! A file is read into [`RuleSpec`]s, which say what the file says, and compiled into an
//! [`Acl`], which decides. An [`Acl`] bound to one client, [`ClientRules`], decides without
//! evaluating a name, a pattern or an address again, and [`AclAuthorizer`] is the
//! `openqtt-ext` authorizer over a replaceable [`Acl`].

mod engine;
mod parse;
pub mod render;
pub mod spec;
mod topic;

#[cfg(test)]
mod naive;
#[cfg(test)]
mod properties;

pub use engine::{Acl, AclAuthorizer, ClientRules};
pub use parse::VERSION;
pub use spec::{
    ActionKind, Cidr, CidrError, Decision, NameMatch, QosSet, RuleSpec, TopicSpec, Who,
};

use crate::Error;

/// The rules of an ACL file, as written, without compiling them.
///
/// # Errors
///
/// [`Error::AclSyntax`] for text that is not TOML, [`Error::Acl`] with every problem of the
/// rules' shape.
pub fn parse_rules(text: &str) -> Result<Vec<RuleSpec>, Error> {
    parse::parse(text).map(|(rules, _)| rules)
}

impl Acl {
    /// Reads and compiles an ACL file.
    ///
    /// # Errors
    ///
    /// [`Error::AclSyntax`] for text that is not TOML; [`Error::Acl`] with every problem
    /// found, each naming its rule and the line of the rule's `[[rule]]`.
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        let (rules, lines) = parse::parse(text)?;
        Self::compile_with(&rules, &|index| parse::rule_name(index, &lines))
    }
}
