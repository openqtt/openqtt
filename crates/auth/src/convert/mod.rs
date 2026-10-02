//! Converting the files of OpenQTT 1.x, as `openqtt convert` does (report R2, rule 30):
//! configuration is converted, not reinterpreted.
//!
//! - [`convert_acl`]: an `acl.conf` into the ACL format of 2.0, with the same decisions
//!   except where R2 says **Changed**, and every rule that conflicts with R2 rules 13 to 16
//!   named, for `--strict` to refuse.
//! - [`convert_authn`]: a user file with plain passwords into a `hashed` bootstrap file,
//!   refusing superusers.

mod acl;
mod authn;
mod erlang;

#[cfg(test)]
mod equivalence;

pub use self::acl::{AclConversion, Note, convert_acl};
pub use self::authn::{AuthnConversion, convert_authn};
