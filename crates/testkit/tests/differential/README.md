# The differential harness

Report R1 marks some statements with the test kind `diff`: the differential runner plays one
script against OpenQTT 2.0 and against OpenQTT 1.x (EMQX 5.8.9, over QUIC in single-stream
mode) and compares what the clients see, and a difference R1 records is part of the expected
result. This directory is that harness, an integration test of `openqtt-testkit`.

OpenQTT 2.0 does not run yet. Until it does, the harness proves itself against the oracle:
two runs of every scenario against OpenQTT 1.x must give the same traces after normalizing,
and those must match the traces kept in `oracle/`.

## Running it

```console
make differential          # two runs against OpenQTT 1.x, compared with each other and with oracle/
make differential-bless    # rewrite oracle/ from two runs that agree, after an intended change
```

Both need Docker, and neither is part of `make check`: the Docker tests are `#[ignore]`d, and
the Makefile targets run them alone with `cargo nextest run --run-ignored only`. The first run
pulls the oracle image, about 110 MB. After that, the two runs take a little over a minute,
most of it waiting out the quiet periods that let a missing packet show as missing. What `make check` does run here
is the bookkeeping: every scenario and every entry of `divergences.toml` names statements,
decisions and open choices that R1 defines, and the two files agree on which scenario shows
which.

Each run writes its traces under `target/tmp/differential/`: `run-a/` and `run-b/` hold the
normalized traces, and `run-a/raw/` and `run-b/raw/` the raw ones, with the time of every
record and nothing replaced. A failure prints a line diff of the trace that differs.

## The oracle

`openqtt_testkit::Oracle` starts `ghcr.io/openqtt/openqtt:1.0.1`, pinned by the digest of its
multi-architecture index (`ORACLE_IMAGE`), with the QUIC listener turned on by environment
variables:

```console
OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLE=true
OPENQTT_LISTENERS__QUIC__DEFAULT__BIND=0.0.0.0:14567
OPENQTT_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__CERTFILE=/opt/openqtt/etc/certs/differential/server.pem
OPENQTT_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__KEYFILE=/opt/openqtt/etc/certs/differential/server.key
```

The certificate and key come from a throwaway CA (`TestPki`) and are copied into the container
before it starts, so the clients verify the broker as they would any other. UDP 14567 is
published on a free port of 127.0.0.1, and the container is removed when the test ends.
Everything else is the image's default configuration: no authentication, the default
authorization rules, `strict_mode` off.

## Scenarios

`scenarios.rs` holds the starter catalogue, one function per scenario. A scenario is a list of
steps for named clients, written with `openqtt_testkit::Scenario`: open a connection, send a
packet or raw bytes, receive, expect, acknowledge whatever arrived, wait, close. `{ns}` in a
topic, filter or Client Identifier is replaced by a namespace unique to each run, so runs never
meet on the broker, and the normalized trace writes `{ns}` back.

Each scenario names the R1 statements it exercises and the decisions, D1 to D32, or open
choices, O-entries, whose difference from EMQX its trace shows. They are data, not `covers:` lines: a scenario proves a
statement only once it runs against OpenQTT 2.0 with the kept trace and the divergences as its
expected result, and `make conformance` should count it from then on, not before.

To add one: write it in `scenarios.rs` and add it to `catalogue()`; list it under each
decision it shows in `divergences.toml`; run `make differential-bless`; and read the trace it
wrote to `oracle/` before committing it, since that trace becomes the expected behaviour of
OpenQTT 1.x. Steps should wait for exactly what OpenQTT 1.x sends, so its runs do not idle, and
end with a short wait wherever a late packet would change the trace.

## Normalized traces

`openqtt_testkit::trace` says what normalizing replaces and why: the run's namespace, Assigned
Client Identifiers, Packet Identifiers (numbered by exchange, `c1` for the client's, `s1` for the
server's), the Topic Aliases the server chose, the server's Reason Strings and the User
Properties on its own packets, time, and the end or reset of the control stream, which races
with the close of the connection. Binary data over 128 bytes is written as its length and a
hash.

## Divergences

`divergences.toml` lists D1 to D32 of R1, and the behaviours the specification leaves open
where OpenQTT chooses otherwise than EMQX and no decision already explains the difference, O4
and O26: what OpenQTT 1.x does, what 2.0 does, the statements each bears on, and the scenarios
whose traces show it. Once 2.0 runs, its traces are compared with `oracle/`, and every
difference must be one these entries explain.

## What OpenQTT 1.x does that R1 does not say

Found while building the harness; each shows in the kept traces or in how the harness had to
be written.

- **It drops any QUIC connection whose client offers datagrams.** quinn offers RFC 9221
  datagrams by default (the `max_datagram_frame_size` transport parameter). The connection
  process of EMQX 5.8.9 then crashes on msquic's datagram state event,
  `undef emqx_quic_connection:dgram_state_changed/3`, and the connection closes with
  application code 0, with or without its CONNACK delivered. MQTT over QUIC uses no datagrams
  (`docs/spec/mqtt-over-quic.md`, section 1), so `openqtt-client` and the test kit offer none.
- **After its own DISCONNECT, it closes the connection three seconds later, with application
  code 1.** After DISCONNECT 0x8D, 0x8F and the like it finishes the control stream at once;
  after DISCONNECT 0x8E, on a takeover, it does not, and sometimes resets the stream in the same
  instant as the close.
- **After the client's DISCONNECT it finishes the control stream and leaves the connection
  open** for the client to close, as `docs/spec/mqtt-over-quic.md`, section 7, asks of the
  sender of DISCONNECT.
- **After discarding a packet too large for a client, it can hold back what it sends that
  client next.** A PUBLISH or a PINGRESP that follows waited from under one second to over
  three, or until the client's DISCONNECT. Its QUIC transport sends each batch of packets
  with `quicer:async_send` and the `?QUICER_SEND_FLAG_SYNC` flag (`emqx_quic_stream.erl`), and a
  batch of discarded packets is an empty send. So `client_maximum_packet_size` sends each
  packet to a subscriber of its own, which only waits to see whether it arrives.
