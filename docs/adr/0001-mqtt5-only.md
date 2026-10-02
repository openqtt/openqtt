# ADR 0001: MQTT 5.0 only

- Status: accepted
- Date: 2026-10-01

## Context

OpenQTT 1.x speaks MQTT 3.1, 3.1.1 and 5.0, because EMQX does. Carrying 3.1.1
into 2.0 would give the codec two grammars and the session two sets of
semantics: clean session against session expiry, no flow control against
Receive Maximum, and acknowledgements with no reason code against
acknowledgements with one.

The last of these decides it. A 3.1.1 PUBACK has no reason code, so a broker
that refuses a QoS 1 publish can either acknowledge it, and the publisher
believes it was accepted, or disconnect the client. MQTT 5 answers with reason
code 0x87, not authorized, and the client knows. The same holds for every
other refusal: subscriptions, quotas, takeover, server shutdown.

## Decision

2.0 implements MQTT 5.0 (OASIS Standard, 7 March 2019) and nothing older.

A CONNECT whose protocol level is not 5 is refused with CONNACK reason code
0x84, unsupported protocol version, and the connection is closed, as
`[MQTT-3.1.2-2]` allows.

Report R1 settles the exact bytes. A 3.1.1 client cannot parse an MQTT 5
CONNACK, so the refusal may go out in the 3.1.1 shape instead (return code
0x01), and a 3.1 CONNECT names the protocol `MQIsdp`, which `[MQTT-3.1.2-1]`
lets the server close without any CONNACK. Either way the connection is
refused.

## Consequences

- Every 1.x client has to speak MQTT 5 before it can connect to 2.0. The 1.x
  line stays maintained for the clients that cannot move yet.
- One grammar in `openqtt-codec`, one set of semantics in `openqtt-session`.
- The broker can always say why: a denied publish gets 0x87, never a silent
  PUBACK, and a client being moved gets DISCONNECT 0x9C (use another server)
  or 0x9D (server moved).
- MQTT 5's flow control, topic aliases, message expiry, session expiry and
  server keep alive are available to the broker's own design rather than
  optional extras.
