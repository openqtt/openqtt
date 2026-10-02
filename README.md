# OpenQTT

**For production, use OpenQTT 1.0.x** (branch `release/1.x`, image tags
`1.0.N`). This branch, `main`, is OpenQTT 2.0: a pre-release rewrite that does
not run as a broker yet.

## What 2.0 is

- An MQTT 5.0 broker written in Rust, which clients reach over QUIC.
- One binary, `openqtt`, with four roles. **edge** holds client connections,
  **router** holds subscription interest, **log** holds durable state
  (sessions, queues, retained messages) and **admin** serves the REST API. Each
  role runs as its own set of pods and scales with what loads it. An
  all-in-one mode runs every role in one process.
- Distributed from the start: sessions are sharded by client id, and no
  cluster-wide structure holds an entry per device.
- Extended at compile time, through the traits in `openqtt-ext`. Nothing is
  loaded at runtime.
- Apache License 2.0.

## What it deliberately is not

- **Not MQTT 3.1 or 3.1.1.** A CONNECT at any other protocol level gets
  CONNACK 0x84, unsupported protocol version. See
  [ADR 0001](docs/adr/0001-mqtt5-only.md).
- **No TCP, TLS or WebSocket listener yet.** QUIC is the only client
  transport. Sessions sit behind a transport seam, so an MQTT 5 over TLS/TCP
  listener can be added later without touching them. See
  [ADR 0002](docs/adr/0002-quic-only-tcp-seam-reserved.md).
- No rule engine, no protocol gateways, no runtime plugins and no dashboard
  web UI.

## Status

| Milestone | Scope | Status |
| --- | --- | --- |
| M1 | Rust workspace and CI; the complete MQTT 5 codec, fuzzed | in progress |
| M2 | Conformance and design reports, specifications, test tooling, measured spikes | not started |
| M3 | Distributed walking skeleton: QoS 0 and 1, retained messages, takeover, chart 2.0.0; `v2.0.0-alpha.1` | not started |
| M4 | Persistent sessions, QoS 2, wills, drain, replication | not started |
| M5 | The rest of MQTT 5, security and operations; `v2.0.0-beta.1` | not started |
| M6 | QUIC in production, the client library, load and soak tests; `v2.0.0-rc.1` | not started |
| M7 | Migration from 1.x; `v2.0.0` | not started |

## Building from source

`rust-toolchain.toml` pins the compiler, and `rustup` installs it on first
use. The gate needs two cargo tools:

```console
cargo install --locked cargo-nextest cargo-deny
make check
```

`make check` is what CI runs: formatting, clippy, the tests, the crate
layering rules, cargo-deny, and a refusal of em dashes in Markdown. The image:

```console
docker build -f deploy/docker/Dockerfile -t openqtt:dev .
```

## Relation to EMQX

OpenQTT 1.x is the source of EMQX 5.8.9, where EMQX's last Apache 2.0 line
stopped, with the product surface renamed. It is maintained on `release/1.x`:
rebuilds, dependency bumps and security fixes, no features. Tag `emqx-v5.8.9`
marks the import and tag `v1.0.0` the first OpenQTT release.

2.0 is a new codebase. It is written from the OASIS MQTT 5.0 specification.
Where EMQX's behaviour informs a decision, it is cited by permalink rather than
copied, and nothing is taken from EMQX 5.9 or later, which is not Apache 2.0.
[ADR 0003](docs/adr/0003-porting-policy.md) is the policy, and a file that does
carry derived portions says so in its header.

EMQX is a trademark of EMQ Technologies Co., Ltd. OpenQTT is not affiliated
with or endorsed by EMQ.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
