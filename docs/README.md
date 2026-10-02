# Documentation

## Architecture decisions

| ADR | Decision |
| --- | --- |
| [0001](adr/0001-mqtt5-only.md) | MQTT 5.0 only |
| [0002](adr/0002-quic-only-tcp-seam-reserved.md) | QUIC only for clients, with a transport seam reserved for TLS/TCP |
| [0003](adr/0003-porting-policy.md) | Porting policy: what may be read and taken from EMQX and elsewhere |

## Reports

Each report answers one design question, from reading the specification and
EMQX or from a measured spike. They go in `reports/`.

| Report | Question | Status |
| --- | --- | --- |
| R1 | Protocol conformance: every MQTT 5.0 normative statement, EMQX's behaviour and ours | to be written |
| [R2](reports/R02-compatibility.md) | Compatibility contract with 1.x, as executable tests | draft |
| [R3](reports/R03-roles-and-wire.md) | Roles, the message path, the wire protocol between roles and the extension traits | draft |
| [R4](reports/R04-log.md) | The log: storage engine, replication, what durable means before a PUBACK, data model (spike S1) | draft |
| R5 | Sessions and handoff: takeover, drain, rolling upgrade, wills | to be written |
| R6 | Routing at scale: interest aggregation, wildcard index, memory, fan-out | to be written |
| R7 | QUIC in a cluster: migration, 0-RTT and load balancers | to be written |
| R8 | Security model: authentication, authorization, revocation, admin API | to be written |
| R9 | Operations contract: metrics, logs, health, admin API and CLI | to be written |
| R10 | Migration from 1.x | to be written |

## Specifications

Normative specifications go in `spec/`.

| Specification | Status |
| --- | --- |
| [`spec/mqtt-over-quic.md`](spec/mqtt-over-quic.md): MQTT 5 over QUIC, compatible with EMQX and NanoMQ | draft |
| `spec/wire.md`: the protocol between roles | to be written |
