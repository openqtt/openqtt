# MQTT 5 over QUIC

Status: draft, part of report R1 and R7. Normative for OpenQTT 2.0.

No OASIS standard defines how MQTT runs over QUIC. This document is OpenQTT's
definition. It is compatible with the mapping EMQX 5.8.9 implements, so EMQX,
NanoMQ and their clients interoperate with OpenQTT in single-stream mode, and in
multi-stream mode where this document says so. References to EMQX behaviour point
at tag `emqx-v5.8.9` of this repository.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Transport

- QUIC version 1 (RFC 9000) with TLS 1.3 (RFC 9001). Version 2 (RFC 9369) MAY be
  negotiated later.
- ALPN `mqtt`. A server MUST refuse a handshake without it. EMQX sets the same
  ALPN (`apps/emqx/src/emqx_listeners.erl`, `alpn => ["mqtt"]`).
- Default UDP port 14567, as EMQX. Deployments facing the internet SHOULD also
  listen on UDP 443, which more firewalls admit.
- Only MQTT 5.0 is spoken. A CONNECT at any other protocol level is refused and the
  connection closes, with the bytes report R1 gives (D1): an MQTT 3.1 or 3.1.1
  client gets the CONNACK of its own version with return code 0x01, and any other
  level gets CONNACK with reason code 0x84 (Unsupported Protocol Version).
- QUIC datagrams (RFC 9221) are not used.

## 2. Streams

### 2.1 The control stream

- The first client-initiated bidirectional stream is the control stream. The client
  MUST send CONNECT as the first packet on it.
- CONNECT, CONNACK, PINGREQ, PINGRESP, DISCONNECT and AUTH travel only on the
  control stream. A packet of one of these types on any other stream is a protocol
  error: the server sends DISCONNECT 0x82 on the control stream and closes the
  connection.
- Closing the control stream, in either direction, ends the MQTT connection.

### 2.2 Single-stream mode

A client that opens only the control stream sends every packet on it. This is the
baseline every OpenQTT client and server MUST support, and the mode the test suites
use against EMQX.

### 2.3 Multi-stream mode

- After CONNACK with reason code below 0x80, a client MAY open further
  bidirectional streams, called data streams.
- Data streams carry PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK,
  UNSUBSCRIBE and UNSUBACK (packet types 3 to 11), the same set EMQX permits
  outbound on a data stream (`emqx_quic_data_stream.erl`,
  `is_datastream_out_pkt/1`).
- Data streams opened before CONNACK are held, not processed, until the
  connection is accepted, as EMQX does (`emqx_quic_connection.erl`,
  `activate_data_streams/2`). If CONNACK refuses the connection, those streams are reset.
- The server never opens streams.
- An acknowledgement travels on the stream that carried the packet it
  acknowledges.
- Messages delivered for a subscription travel on the stream that carried the
  SUBSCRIBE that created it. A subscription made on the control stream delivers on
  the control stream. A message that matches subscriptions made on different
  streams goes out once (report R1, O11), on the stream of the matching
  subscription with the highest granted QoS, the oldest among equals.
- Packet identifiers are scoped to the session, not to a stream. A client MUST NOT
  reuse an in-flight identifier on another stream.
- Ordering is guaranteed only within a stream. MQTT's ordering rules (section 4.6
  of the MQTT 5.0 specification) apply per stream.
- Receive Maximum and the inflight window are counted per session, across all
  streams.
- Topic Aliases travel on the control stream only, in both directions. A PUBLISH
  carrying a Topic Alias on a data stream is a protocol error: the receiver sends
  DISCONNECT 0x82 on the control stream and closes the connection. MQTT 5 scopes
  an alias mapping to the connection and assumes one ordered stream; across
  streams, a PUBLISH could use an alias before the PUBLISH that sets it arrives.

### 2.4 Ending a data stream

- A client ends a data stream by finishing or resetting its sending side, or by
  stopping the server's (STOP_SENDING). QoS 1 and 2 packets are never
  retransmitted on another stream of the same connection: MQTT allows
  retransmission only when a session resumes on a new network connection
  ([MQTT-4.4.0-1]).
- If the stream still has an exchange that needs a packet from the client, the
  server MUST send DISCONNECT 0x82 on the control stream and close the connection.
  Such an exchange is a QoS 1 or 2 PUBLISH the server sent on the stream that the
  client has not fully acknowledged, or a QoS 2 PUBLISH the client sent on the
  stream whose PUBREL the server has not received. Retransmission then follows
  the usual rules when the client reconnects.
- An acknowledgement the server still owes on the stream (PUBACK, PUBREC,
  PUBCOMP, SUBACK or UNSUBACK) is sent if the server's sending side is still
  open. If the client has stopped that side, the acknowledgement can no longer
  be delivered, and the client would hold the packet identifier, and for a
  PUBLISH a slot of its send quota, for the rest of the connection. So the
  server MUST then send DISCONNECT 0x82 on the control stream and close the
  connection, and the exchange recovers when the client reconnects.
- Otherwise only the stream ends, and the server finishes its side.
  Subscriptions made on the stream deliver on the control stream from then on.
- On a resumed session every subscription delivers on the control stream, and
  retransmitted PUBLISH and PUBREL packets go there too, until a SUBSCRIBE on a
  data stream replaces a subscription and moves its deliveries to that stream.

## 3. Identity and TLS

- The server MAY require a client certificate. When it does, it MUST verify the
  chain against the configured issuing CA only, and MUST check the clientAuth
  extended key usage. EMQX 5.8.9 cannot read the peer certificate on QUIC
  (`emqx_quic_stream.erl`, `peercert/1` returns `nossl`); OpenQTT can, and
  certificate-derived identity (the subject CN as username) is defined only here.
- On a resumed TLS session the certificate was verified when the ticket was issued.
  The server MUST still re-check bans and revocation for the identity bound to the
  ticket before CONNACK.

## 4. 0-RTT

- A server MAY accept 0-RTT data. CONNECT MAY arrive in 0-RTT.
- A server MUST NOT act on PUBLISH, SUBSCRIBE or UNSUBSCRIBE received in 0-RTT
  until the handshake confirms the early data was accepted, because 0-RTT data can
  be replayed. It queues them in arrival order and processes them after
  confirmation, or discards them if early data is rejected (the client then
  retransmits per QUIC).
- CONNACK MUST NOT be sent before the handshake completes.

## 5. Connection migration

- Servers SHOULD keep a connection when the client's address changes (RFC 9000
  section 9). The MQTT session is unaffected by a migration.
- Behind a load balancer, connection IDs encode the serving node so a migrated
  packet can reach it. How is defined in report R7.
- A server MAY name a preferred address during the handshake. Clients SHOULD
  migrate to it.

## 6. Keepalive and timeouts

- MQTT Keep Alive is the liveness rule. QUIC idle timeout MUST be longer than
  1.5 times the negotiated Keep Alive, so QUIC never closes a connection MQTT
  still considers alive.
- QUIC PINGs MAY be used to keep NAT bindings open below the MQTT keepalive
  interval.

## 7. Closing

- A clean end is DISCONNECT on the control stream, then a QUIC CONNECTION_CLOSE
  with application error code 0.
- An abrupt QUIC close without DISCONNECT is an abnormal disconnect: the will
  message, if any, is published.
- When the server moves a client it sends DISCONNECT with 0x9C (Use another
  server) or 0x9D (Server moved), with a Server Reference property when it has one.

## 8. Error codes

QUIC application error codes used in CONNECTION_CLOSE and RESET_STREAM:

| Code | Meaning |
| --- | --- |
| 0x0 | No error, after DISCONNECT |
| 0x1 | Protocol error (mirrors MQTT 0x82) |
| 0x2 | Internal error |
| 0x3 | Stream refused (data stream opened before the connection was accepted, then refused) |

EMQX does not define these; a client that does not know them treats any nonzero
code as an abnormal close.
