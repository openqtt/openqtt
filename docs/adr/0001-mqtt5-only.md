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

A CONNECT that is not MQTT 5.0 is refused and the connection is closed, in
bytes the client can read (report R1, decision D1):

| Protocol Name | Protocol Version | Reply, then close |
| --- | --- | --- |
| `MQTT` or `MQIsdp` | 3 or 4 (also 0x83, 0x84) | CONNACK return code 0x01, `20 02 00 01` |
| `MQTT` or `MQIsdp` | any other but 5 | CONNACK reason code 0x84, `20 03 00 84 00` |
| anything else | any | none |

A 3.1 or 3.1.1 client cannot parse the MQTT 5 CONNACK, so it gets the refusal
its own version defines. `[MQTT-3.1.2-1]` and `[MQTT-3.1.2-2]` allow all
three.

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
