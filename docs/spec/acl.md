# ACL rules

Status: draft, part of report R8. Normative for the authorization built into OpenQTT 2.0
(`openqtt-auth`): the file `auth.acl_file` names (`docs/spec/config.md`), how it is read, and
how it decides.

## Deciding

The rules are read in order. The first rule whose clients, action and topics all match the
request decides it, allow or deny; when no rule matches, the request is denied (R2 rule 9).
Without a file there are no rules, so everything is denied.

Rules see topics as the client writes them, before the listener's mountpoint is put in front
(R2 rule 7). What is decided:

| Request | Checked as | Refused with |
| --- | --- | --- |
| A PUBLISH, or a CONNECT's Will Message | `publish`, its Topic Name, QoS and RETAIN | PUBACK or PUBREC 0x87; a QoS 0 PUBLISH is dropped and counted (R1 O14) |
| One filter of a SUBSCRIBE | `subscribe`, its filter without `$share/{ShareName}/`, and its Maximum QoS | 0x87 in the SUBACK, for that filter alone (R2 rule 12) |
| A delivery, where the edge checks deliveries | `subscribe`, its Topic Name and the QoS it is sent at | not delivered |

## The file

TOML. A rule is a table in the array `rule`, written `[[rule]]`:

```toml
version = 1

# Services, named by the prefix the user list reserves for them.
[[rule]]
permission = "allow"
username = { prefix = "svc:" }
address = "10.0.0.0/8"
action = "all"
topics = ["#"]

# Every client may publish under its own name, without RETAIN, at QoS 0 or 1.
[[rule]]
permission = "allow"
action = "publish"
qos = [0, 1]
retain = false
topics = ["users/${username}/#"]
```

| Key | Value |
| --- | --- |
| `version` | Optional. `1`, the only version. |
| `rule` | The rules, in order. |

A key the format does not know is an error, which names the nearest known key.

| Rule key | Value | Required |
| --- | --- | --- |
| `permission` | `"allow"` or `"deny"` | yes |
| `action` | `"publish"`, `"subscribe"`, or `"all"` for both | yes |
| `topics` | a list of topics, at least one; see [Topics](#topics) | yes |
| `qos` | a QoS level, or a list of them, from 0 to 2: the rule applies only to these. Without it, to all three | no |
| `retain` | `true` or `false`: the rule applies only to a publish with RETAIN set, or only to one without. Not with `"subscribe"`; with `"all"`, it narrows the publishes and leaves subscriptions alone | no |
| `username` | the clients by user name; see [Clients](#clients) | no |
| `client_id` | the clients by Client Identifier | no |
| `address` | the clients by address | no |
| `attributes` | the clients by the attributes authentication gave them | no |

## Clients

A rule applies to a client when every client key it has matches. A rule with none applies to
every client.

`username`, `client_id` and each attribute of `attributes` take a matcher, or a list of
matchers that matches when any of them does:

| Matcher | Matches a name that |
| --- | --- |
| `"text"` | is exactly `text`, byte for byte |
| `{ prefix = "text" }` | begins with `text`, which is not empty |
| `{ regex = "pattern" }` | matches `pattern` as a whole, in the syntax of Rust's regex crate. Matching takes time linear in the name whatever the pattern, so no client name can make a rule slow. `dev-[0-9]+` matches `dev-17` and not `dev-17x` or `my-dev-17` |

- `username` is the principal's name: the certificate's CN on a listener configured for
  certificate identity (R2 rule 4), the user the password was checked for on a password
  listener, the name the client gave on a listener that does not authenticate. A client
  without one matches no rule that has `username`.
- `client_id` is the Client Identifier the session is kept under: the one the client sent, the
  one it was assigned, or its CN. On a password listener the client chooses it, so it narrows a
  rule and should not grant one alone (R2 rule 15).
- `address` is a network, `"10.0.0.0/8"` or `"2001:db8::/32"`, a single address, or a list of
  them. An address with bits set past the prefix, such as `"10.0.0.5/8"`, is an error that
  names the network meant. An IPv4 client reaching an IPv6 socket, `::ffff:10.1.2.3`, is
  compared as `10.1.2.3`. A client without an address matches no rule that has `address`.
- `attributes` is a table from an attribute's name to a matcher, every one of which must
  match. A client without the attribute does not match.

**An allow rule cannot name `address` alone** (R2 rule 14): the address narrows a rule that
names who the client is (`username`, `client_id` or `attributes`) and grants nothing by
itself. A deny rule may name only an address.

## Topics

Each entry of `topics` is one of:

| Entry | Matches |
| --- | --- |
| `"filter"` | a topic filter: `+` matches one level and `#` any number, the parent level included, as MQTT defines them. A filter beginning with a wildcard never matches a topic beginning with `$` ([MQTT-4.7.2-1]): a `$`-topic matches only a rule that names its first level, `"$SYS/#"` (R2 rule 10) |
| `{ eq = "text" }` | a topic or filter that is exactly `text`, wildcards included, so `{ eq = "#" }` is the subscription to `#` and nothing else. No placeholders |
| `{ all = true }` | every topic, `$`-topics included |

A filter may use `${username}` and `${clientid}`, anywhere in its text, for the client's own
values, so `"users/${username}/#"` gives each client a namespace. A value may contain `/`,
which makes more levels, as a CN can. For one client the filter matches nothing when the
client has no value for a placeholder it uses, or when a value is empty, contains `+`, `#` or
U+0000, or begins the filter with `$`: a client named `#` cannot turn `${username}/x` into `#/x`,
and one named `$SYS` cannot reach `$SYS/x`. Other placeholders are an error.

A filter of a rule cannot begin with `$share/`: a subscription is checked without its
`$share/{ShareName}/`, so the group grants nothing.

### Subscriptions

A subscription's filter is checked as a topic name against the rule's filter (R2 rule 11): its
levels are text, compared level by level.

- A literal level of the rule matches the same text only. The rule `a/b` does not allow `a/+`.
- An allow rule's `+` matches any one level but `#`, since a `#` stands for any number of
  levels, the parent level included. Only the rule's `#` allows it.
- A deny rule's `+` matches a `#` as well, as 1.x read it: the subscription `a/#` receives what
  `a/+` names, so a deny of `a/+` refuses it, and a deny never refuses less than it did in 1.x.
- The rule's `#` matches the rest, nothing included.
- A subscription beginning with `$` is matched only by a rule that names its first level.

So a rule allowing `ingest/acme/+/+/+` allows that filter, and `ingest/acme/a/+/c`, and refuses
the broader `ingest/acme/#`, `ingest/+/+/+/+` and `ingest/acme/+/+/#`. The comparison is level
by level and errs towards refusal: the rule `+/#` matches every topic `#` does, and still does
not allow the subscription `#`.

### Deliveries

A delivery is decided as a subscription to its Topic Name, at the QoS it is delivered at. A
client may receive what it could subscribe to, so a deny rule narrower than an allow rule
still keeps its topics from a subscriber whose filter the allow rule let through:

```toml
[[rule]]
permission = "deny"
action = "subscribe"
topics = ["a/secret"]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["a/#"]
```

The subscription `a/#` is allowed; where the edge checks deliveries, `a/secret` is not
delivered through it.

## The device pattern

The rules R2 (rules 13 to 16) asks of a fleet of devices identified by certificates, with the
mountpoint `ingest/${username}/` and services named by a reserved prefix:

```toml
# Services, by the prefix only service credentials carry (R2 rule 15), and only from inside
# the cluster: the address narrows the name and grants nothing by itself (rule 14).
[[rule]]
permission = "allow"
username = { prefix = "svc:" }
address = "10.0.0.0/8"
action = "all"
topics = ["#"]

# A device never sends commands, retained or not (rule 13).
[[rule]]
permission = "deny"
action = "publish"
topics = ["commands/#"]

# A device never sets RETAIN (rule 16).
[[rule]]
permission = "deny"
action = "publish"
retain = true
topics = [{ all = true }]

[[rule]]
permission = "allow"
action = "publish"
topics = ["telemetry/#", "events/#"]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["commands/#"]
```

Rule 15 is kept by authentication as much as by the rules: the password list gives the reserved
prefix only to service credentials, a certificate whose CN begins with it is refused, and so is
a client naming itself with it on a listener that does not authenticate. A service named
exactly rather than by the prefix is safe only where no other authenticator can produce that
name.

## Errors

The whole file is checked before it is used, and every problem is reported at once, one per
line, naming its rule by number and by the line of its `[[rule]]`:

```text
rule 3 (line 17): unknown key `usrname` in a rule; the nearest is `username`
rule 3 (line 17): `address` "10.0.0.5/8" has bits set past its prefix; the network is "10.0.0.0/8"
```

A file that is not TOML is reported by line and column, without the line itself.

## How it is decided

The rules are compiled once: patterns, networks and filters parsed, and each rule's action,
QoS and RETAIN made into a set of bits. When a client connects they are bound to it: which
rules apply to it, from its names, address and attributes, and its own values in place of each
placeholder. A decision then walks the rules in order, passes over one that does not apply to
the client or the request with two bit tests, rules out most topics by a key packed from their
first level, and compares the rest. No pattern, network or placeholder is evaluated per
message. When the rules are replaced, a connected client binds again at its next decision.

`cargo test --release -p openqtt-auth --test throughput -- --ignored --nocapture` measures
decisions per second over a file of 100 rules.
