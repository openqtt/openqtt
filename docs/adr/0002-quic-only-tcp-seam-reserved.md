# ADR 0002: QUIC only for clients, with a transport seam reserved

- Status: accepted
- Date: 2026-10-01

## Context

The clients a broker like this serves are devices on mobile, lossy and
changing networks. QUIC suits them: a connection survives an address change,
a returning client can resume in 0-RTT, a lost packet on one stream does not
stall the others, and TLS 1.3 is not optional. One client transport also means
one listener to secure, test and operate.

There is no OASIS standard for MQTT over QUIC. EMQX and NanoMQ both implement
it the same way: ALPN `mqtt`, the first bidirectional stream carries the
control packets, and a single stream is the baseline every client can rely on.
OpenQTT's mapping, compatible with that baseline, is to be specified in
`docs/spec/mqtt-over-quic.md`.

The cost is real. Some networks block UDP, and most MQTT tools and client
libraries speak only TCP, TLS or WebSocket.

## Decision

The client listener in 2.0 is QUIC, on quinn with rustls. There is no TCP, TLS
or WebSocket listener.

The transport is kept behind a seam. `openqtt-transport` yields an
`MqttConnection`: ordered bytes in each direction, the peer's certificate chain
and address, whether data arrived in 0-RTT, and a way to close with a reason.
`openqtt-session` is sans-IO and `openqtt-edge` drives it through that seam,
so neither names a quinn type. An MQTT 5 over TLS/TCP listener is then a second
implementation of the seam, with 0-RTT and migration always absent, and no
change to sessions.

That listener is not built now. It is built before 2.0.0 only if measurement
shows a meaningful share of clients on networks that block UDP, and otherwise
later, on demand. WebSocket is not planned; browsers would come back through
WebTransport if they come back at all.

## Consequences

- `openqtt-client` speaks MQTT 5 over QUIC, and so does every client of 2.0
  until the TLS/TCP listener exists. Standard MQTT tools cannot connect to it.
- No type from quinn or rustls appears in `openqtt-session`; `make layers`
  enforces it.
- Mutual TLS, certificate identity and the client's real source address come
  from the QUIC handshake and packets. The PROXY protocol and TLS-PSK have no
  role.
- Running QUIC behind load balancers, with migration and 0-RTT across a
  cluster, is its own problem. Report R7 measures it before the design is
  settled.
