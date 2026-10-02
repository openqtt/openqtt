//! Built-in authentication and authorization.
//!
//! A password list, JWT, client identity from a certificate (its CN, and the clientAuth extended
//! key usage), and an ACL engine over a TOML rule file: the first matching rule wins, and no
//! match denies. Also converts the `acl.conf` and `authn.csv` files of OpenQTT 1.x.
//!
//! It makes no network calls, so it must not depend on hyper or reqwest. Authentication over
//! HTTP belongs to the edge.
