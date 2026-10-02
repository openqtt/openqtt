# R1. Protocol conformance

Status: draft. Every numbered normative statement of MQTT 5.0, what EMQX 5.8.9 does
with it, and what OpenQTT 2.0 will do. Once accepted it is normative for the codec and the
session machine, and it is the record ADR 0003 asks for: every place where OpenQTT departs
from the specification or from EMQX is written here.

## How to read this report

The source is the OASIS standard *MQTT Version 5.0* of 7 March 2019,
<https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html>. Its text is not reproduced:
each statement is paraphrased in one line, so read the standard alongside. Its appendix B
lists 251 numbered statements, tagged `[MQTT-x.y.z-n]`:

| Chapter of the standard | Statements | In this report |
| --- | --- | --- |
| 1 Introduction | 5 | table 1 |
| 2 MQTT Control Packet format | 7 | table 2 |
| 3 MQTT Control Packets | 175 | table 3 |
| 4 Operational behavior | 60 | table 4 |
| 5 Security | 0 | none; R8 is the security model |
| 6 Using WebSocket as a network transport | 4 | none: out of scope, since OpenQTT has no WebSocket transport (ADR 0002) |

Each of the 247 statements of chapters 1 to 4 has one row, in numeric order, which is the
standard's section order. Chapter 6
holds MQTT-6.0.0-1 to MQTT-6.0.0-4, which bind only a WebSocket transport, so they are not
tracked. Two statements are tagged wrongly in the standard's body, as `[MQTT-4.2-1]` and as a
truncated `[MQTT-3.2.2-8]`; this report uses appendix B's ids, MQTT-4.2.0-1 and MQTT-3.2.2-8.

The columns:

- **Statement**: the id, without brackets.
- **Requires**: a paraphrase. A SHOULD or MAY inside a numbered statement stays one.
- **Layer**: the crate that owns the behaviour, `openqtt-<layer>`, with R3 describing the roles:
  `codec`, `topic`, `session`, `edge`, `log`, `router`, `auth`, `transport`, and `client` for
  an obligation that falls on the client alone. The broker's answer to a client that breaks
  such an obligation is in the OpenQTT column.
- **EMQX 5.8.9**: what OpenQTT 1.x does in its default configuration, read from the source at
  tag `emqx-v5.8.9`. *Conforms* means the reading found nothing against the statement;
  *deviates* says how, with a permalink of the form
  `https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/<path>#L<a>-L<b>`. Notable cases that
  conform carry a link too. Many deviations come from one setting, `mqtt.strict_mode`, which is
  off by default and gates many of the checks the standard asks for [emqx_schema.erl L3717-3724][sc-strict]. This column
  was read, not observed: the differential tests confirm it.
- **OpenQTT**: `conform`, or `deviate` with the reason. A row is `deviate` when OpenQTT departs
  from the statement in any configuration it supports, and the cell names the configuration.
  D*n* refers to the decisions that differ from EMQX and O*n* to the behaviour the
  specification leaves open, both below. R2 is the compatibility contract and R3 the roles
  report.
- **Test**: the kind of test that will prove the row.
  - `unit`: a test in the owning crate with no broker: codec round trips and refusals, topic
    validation and matching, client behaviour.
  - `session`: the sans-IO session machine driven in-process, with inputs and a clock in and
    effects out.
  - `e2e`: a running broker, all-in-one or the in-process cluster of `openqtt-testkit`, driven
    over QUIC by the test kit's raw-packet client.
  - `diff`: the differential runner plays one script against OpenQTT 2.0 and against OpenQTT
    1.x (EMQX 5.8.9, over QUIC in single-stream mode) and compares what the clients see. A
    difference this report records is part of the expected result.

## Tests carry statement ids

A test that proves a statement names it: its name is `mqtt_<x>_<y>_<z>_<n>_` followed by what
it checks, starting with a letter.

```rust
#[test]
fn mqtt_3_1_2_2_a_3_1_1_connect_gets_return_code_1() {
    // ...
}
```

A test that proves further statements lists them on a comment line that starts `covers:`.
Scenario files for the test kit use the same line.

```rust
// covers: MQTT-3.2.2-6, MQTT-3.2.2-7
```

Code that implements a statement cites it as `[MQTT-x.y.z-n]`, as ADR 0003 asks; a citation is
not a test and is not counted. `scripts/conformance-ids.sh` reads the ids from the tables of
this report and searches the test tree, then prints every id no test names, one per line, and
a count; `make conformance` runs it. Today it prints all 247.

## Table 1. Chapter 1, data representation

Section 1.5: strings and integers on the wire. The codec owns all five.

| Statement | Requires | Layer | EMQX 5.8.9 | OpenQTT | Test |
| --- | --- | --- | --- | --- | --- |
| MQTT-1.5.4-1 | String character data is well-formed UTF-8 and encodes no surrogate (U+D800 to U+DFFF) | codec | deviates by default: strings are checked only with `strict_mode`, which is off [emqx_frame.erl L713-716][f-utf8] [emqx_schema.erl L3717-3724][sc-strict]; topic names, filters and the will topic are still checked by `emqx_topic:validate` [emqx_topic.erl L224-261][t-validate] | conform: every UTF-8 string field is checked on decode; ill-formed data is a Malformed Packet (D3) | unit |
| MQTT-1.5.4-2 | A UTF-8 string encodes no U+0000 | codec | deviates by default: the same `strict_mode` gate; U+0000 is refused in topics only [emqx_topic.erl L224-261][t-validate] | conform: refused in every string field (D3) | unit |
| MQTT-1.5.4-3 | The bytes EF BB BF are the character U+FEFF and are never skipped or stripped | codec | conforms | conform: strings are kept byte for byte | unit |
| MQTT-1.5.5-1 | A Variable Byte Integer uses the fewest bytes that can hold its value | codec | conforms when encoding [emqx_frame.erl L1146-1149][f-vbi-out]; decoding accepts longer encodings [emqx_frame.erl L642-651][f-vbi] | conform: encode minimally, and refuse a longer encoding on decode as a Malformed Packet, so every packet has exactly one encoding (D3) | unit |
| MQTT-1.5.7-1 | Both strings of a UTF-8 String Pair are valid UTF-8 Encoded Strings | codec | deviates by default: pairs are checked only with `strict_mode` [emqx_frame.erl L664-673][f-pair] | conform (D3) | unit |

## Table 2. Chapter 2, packet format

Fixed-header flags, packet identifiers and properties.

| Statement | Requires | Layer | EMQX 5.8.9 | OpenQTT | Test |
| --- | --- | --- | --- | --- | --- |
| MQTT-2.1.3-1 | Fixed-header flag bits marked reserved carry exactly the listed values | codec | deviates by default: flags are checked only with `strict_mode` [emqx_frame.erl L132-144][f-header] [emqx_frame.erl L1161-1179][f-validate-header], and PUBREL, SUBSCRIBE and UNSUBSCRIBE sent with flags 0000 are taken as 0010 [emqx_frame.erl L1241-1244][f-fixqos] | conform: any other value is a Malformed Packet (D3) | unit |
| MQTT-2.2.1-2 | A QoS 0 PUBLISH carries no Packet Identifier | codec | conforms | conform: the QoS 0 form of PUBLISH has no identifier field | unit |
| MQTT-2.2.1-3 | A client gives each new SUBSCRIBE, UNSUBSCRIBE and QoS 1 or 2 PUBLISH a non-zero identifier not in use | client | server side checks identifier 0 only with `strict_mode`; by default a PUBLISH with identifier 0 is accepted and acknowledged [emqx_frame.erl L394-407][f-publish] | conform: `openqtt-client` allocates from free identifiers; the server answers identifier 0 with DISCONNECT 0x82 (D3) | unit |
| MQTT-2.2.1-4 | The server gives each new QoS 1 or 2 PUBLISH a non-zero identifier not in use | session | deviates in a corner: identifiers come from a wrapping counter that never checks use [emqx_session_mem.erl L815-818][m-pktid]; meeting one still in flight raises in `gb_trees:insert` [emqx_inflight.erl L69-71][i-insert] and drops the connection | conform: take the next identifier not in flight; with at most Receive Maximum in flight one is always free (D7) | session |
| MQTT-2.2.1-5 | PUBACK, PUBREC, PUBREL and PUBCOMP carry the identifier of the PUBLISH they answer | session | conforms | conform | session |
| MQTT-2.2.1-6 | SUBACK and UNSUBACK carry the identifier of their SUBSCRIBE or UNSUBSCRIBE | session | conforms | conform | session |
| MQTT-2.2.2-1 | A packet without properties carries a Property Length of zero | codec | conforms; decoding checks neither that a property is allowed in the packet type nor that a single-valued one appears once [emqx_frame.erl L567-640][f-props] | conform: a property not allowed in the packet type is a Malformed Packet and a repeated single-valued one a Protocol Error (D3) | unit |

## Table 3. Chapter 3, the control packets

In the standard's order: CONNECT (3.1), CONNACK (3.2), PUBLISH (3.3), the four acknowledgements (3.4 to 3.7), SUBSCRIBE and SUBACK (3.8, 3.9), UNSUBSCRIBE and UNSUBACK (3.10, 3.11), PINGREQ (3.12), DISCONNECT (3.14) and AUTH (3.15). PINGRESP (3.13) has no numbered statement.

| Statement | Requires | Layer | EMQX 5.8.9 | OpenQTT | Test |
| --- | --- | --- | --- | --- | --- |
| MQTT-3.1.0-1 | The first packet a client sends is CONNECT | session | conforms: any other first packet closes the connection without a reply [emqx_channel.erl L453-457][c-not-connected] | conform: close without a reply | session |
| MQTT-3.1.0-2 | A second CONNECT is a Protocol Error and closes the connection | session | conforms: DISCONNECT 0x82 [emqx_channel.erl L380-387][c-second-connect] | conform: DISCONNECT 0x82, then close | session |
| MQTT-3.1.2-1 | The protocol name is `MQTT`; a server may answer CONNACK 0x84 to reveal itself, and then closes | codec | accepts `MQTT` and the 3.1 name `MQIsdp`; any other name closes without a reply [emqx_frame.erl L1189-1200][f-protoname] | conform: a 3.1 CONNECT (`MQIsdp`, level 3) gets the refusal `20 02 00 01`, a name other than `MQTT` or `MQIsdp` a close without a reply (D1) | e2e |
| MQTT-3.1.2-2 | A protocol version other than 5 may get CONNACK 0x84; the server then closes | codec | accepts versions 3, 4 and 5, taken from the low four bits of the level byte, so 0x85 is version 5 sent by a bridge [emqx_frame.erl L541-545][f-bridge]; any other version gets return code 0x01 in a two-byte CONNACK [emqx_packet.erl L320-330][p-protover] [emqx_reason_codes.erl L160-182][rc-compat] | conform: levels 3 and 4, with or without the bridge bit, get `20 02 00 01`, and any other level, 0x85 included, `20 03 00 84 00`, then close (D1) | e2e |
| MQTT-3.1.2-3 | The reserved CONNECT flag is 0; otherwise the packet is malformed | codec | conforms in closing [emqx_frame.erl L1202-1214][f-reserved], but every malformed MQTT 5 CONNECT is answered CONNACK 0x95 Packet too large [emqx_channel.erl L1246-1263][c-connect-0x95] | conform: CONNACK 0x81, then close (D4) | unit |
| MQTT-3.1.2-4 | Clean Start 1 discards any existing session and starts a new one | log | conforms [emqx_cm.erl L297-349][cm-open] | conform: the claim at the client's log partition replaces `sess/{cid}` and its queues (R3) | e2e |
| MQTT-3.1.2-5 | Clean Start 0 with an existing session resumes from its state | log | conforms [emqx_cm.erl L297-349][cm-open] | conform: the claim returns the session snapshot to the new edge (R3) | e2e |
| MQTT-3.1.2-6 | Clean Start 0 without a session creates a new one | log | conforms | conform | e2e |
| MQTT-3.1.2-7 | Will Flag 1 stores a Will Message with the session | log | conforms: stored once authentication succeeds, in the connection process [emqx_channel.erl L555-592][c-connect] | conform: kept in `own/{cid}` at the log, so a will outlives its edge (R3) | e2e |
| MQTT-3.1.2-8 | The will is published after the connection closes, once the Will Delay passes or the session ends, unless DISCONNECT 0x00 or a reconnect within the delay removed it | session | conforms [emqx_channel.erl L3063-3081][c-will-delay] [emqx_channel.erl L3029-3062][c-will-takeover] [emqx_channel.erl L2998-3028][c-will-terminate] | conform: the session machine decides; the log publishes the wills of an edge whose epoch dies (R3, O23) | session |
| MQTT-3.1.2-9 | Will Flag 1 requires Will Properties, Will Topic and Will Payload in the payload | codec | conforms in closing; the reply is CONNACK 0x95 [emqx_channel.erl L1246-1263][c-connect-0x95] | conform: Malformed Packet, CONNACK 0x81 (D4) | unit |
| MQTT-3.1.2-10 | The will leaves the session state once published or on DISCONNECT 0x00 | session | conforms [emqx_channel.erl L1062-1068][c-will-clean] | conform | session |
| MQTT-3.1.2-11 | Will Flag 0 requires Will QoS 0 | codec | conforms in closing [emqx_frame.erl L1202-1214][f-reserved]; the reply is CONNACK 0x95 [emqx_channel.erl L1246-1263][c-connect-0x95] | conform: Malformed Packet, CONNACK 0x81 (D4) | unit |
| MQTT-3.1.2-12 | With Will Flag 1, Will QoS is 0, 1 or 2 | codec | conforms: Will QoS 3 is refused [emqx_frame.erl L1202-1214][f-reserved] | conform: Will QoS 3 is a Malformed Packet | unit |
| MQTT-3.1.2-13 | Will Flag 0 requires Will Retain 0 | codec | conforms [emqx_frame.erl L1202-1214][f-reserved] | conform | unit |
| MQTT-3.1.2-14 | Will Retain 0 publishes the will as a non-retained message | session | conforms | conform | diff |
| MQTT-3.1.2-15 | Will Retain 1 publishes the will as a retained message | log | conforms | conform: the will is written to `ret/{topic}` like any retained publish (D15) | diff |
| MQTT-3.1.2-16 | User Name Flag 0 means no User Name in the payload | codec | conforms: trailing bytes are refused [emqx_frame.erl L359-367][f-trailing] | conform | unit |
| MQTT-3.1.2-17 | User Name Flag 1 means a User Name is present | codec | conforms | conform | unit |
| MQTT-3.1.2-18 | Password Flag 0 means no Password in the payload | codec | conforms [emqx_frame.erl L359-367][f-trailing] | conform | unit |
| MQTT-3.1.2-19 | Password Flag 1 means a Password is present | codec | conforms | conform: a Password without a User Name is accepted, as MQTT 5 allows | unit |
| MQTT-3.1.2-20 | With a non-zero Keep Alive, a client with nothing else to send sends PINGREQ | client | client obligation; the server side is MQTT-3.1.2-22 | conform: `openqtt-client` pings within the negotiated interval | unit |
| MQTT-3.1.2-21 | A client given Server Keep Alive uses it instead of its own Keep Alive | client | client obligation | conform | unit |
| MQTT-3.1.2-22 | With a non-zero Keep Alive, the server closes the connection after 1.5 times Keep Alive without a packet, as if the network had failed | session | deviates in timing: idleness is sampled every check interval (`keepalive_check_interval`, 30 s, at most half the Keep Alive), so the close comes between 1.5 and 2 times Keep Alive [emqx_keepalive.erl L75-94][k-init] [emqx_keepalive.erl L138-158][k-check] [emqx_schema.erl L3760-3767][sc-ka-check]; DISCONNECT 0x8D goes first [emqx_channel.erl L1724-1735][c-keepalive-timeout] | conform: a deadline reset by every packet fires at 1.5 times Keep Alive (R2 rule 24); DISCONNECT 0x8D, then close as a failure, so the will is published (D5) | session |
| MQTT-3.1.2-23 | Client and server keep the session after the connection closes when Session Expiry Interval is above 0 | log | conforms; the session lives in memory on the node that held the connection and is lost if that node stops | conform: `sess/{cid}` is committed to the log and survives the loss of the edge (R3) | e2e |
| MQTT-3.1.2-24 | The server never sends a packet larger than the client's Maximum Packet Size | codec | conforms, but also drops a packet exactly at the limit, since the test is `>=` [emqx_frame.erl L1151-1154][f-toolarge] [emqx_frame.erl L751-761][f-serialize] | conform: the encoder measures the whole packet and allows a packet equal to the limit (D6) | unit |
| MQTT-3.1.2-25 | A packet too large to send is discarded and the message treated as if sent | session | deviates: the connection drops the packet [emqx_connection.erl L879-891][conn-drop] after the session put a QoS 1 or 2 message in flight [emqx_session_mem.erl L525-550][m-deliver], so the message keeps its window slot until the session ends or a later connection raises the limit | conform: the session checks the size before sending, completes the message and frees the slot (D6) | session |
| MQTT-3.1.2-26 | The server sends no Topic Alias above the client's Topic Alias Maximum | session | conforms [emqx_channel.erl L2380-2421][c-alias-out] [emqx_channel.erl L2818-2830][c-alias-max] | conform (O6) | session |
| MQTT-3.1.2-27 | Without a client Topic Alias Maximum, or with 0, the server sends no Topic Alias | session | conforms [emqx_channel.erl L2818-2830][c-alias-max] | conform | session |
| MQTT-3.1.2-28 | Request Response Information 0 means no Response Information in CONNACK | session | conforms [emqx_channel.erl L2745-2763][c-resp-info] | conform: Response Information is never sent (O18) | session |
| MQTT-3.1.2-29 | Request Problem Information 0 keeps Reason Strings and User Properties off every packet but PUBLISH, CONNACK and DISCONNECT | session | conforms: Reason Strings appear only on CONNACK and DISCONNECT | conform (O20) | session |
| MQTT-3.1.2-30 | After sending an Authentication Method, a client sends only AUTH or DISCONNECT until CONNACK | client | server side deviates: another packet during the exchange gets DISCONNECT 0x82 before any CONNACK [emqx_channel.erl L453-457][c-not-connected], against MQTT-3.14.0-1 | conform: `openqtt-client` waits; the server answers such a packet with CONNACK 0x82 and closes (D4) | unit |
| MQTT-3.1.3-1 | CONNECT payload fields come in the order Client Identifier, Will Topic, Will Message, User Name, Password | codec | conforms | conform | unit |
| MQTT-3.1.3-2 | Client and server use the ClientID to identify the session state | session | conforms by default; `peer_cert_as_clientid`, `use_username_as_clientid` and `clientid_override` replace the ClientID without telling the client [emqx_channel.erl L2768-2778][c-assigned-prop] | deviate on listeners configured for certificate identity: the certificate's subject CN names the session and is returned as Assigned Client Identifier (R2 rule 4, D20) | e2e |
| MQTT-3.1.3-3 | The ClientID is present, as the first payload field | codec | conforms | conform | unit |
| MQTT-3.1.3-4 | The ClientID is a UTF-8 Encoded String | codec | deviates by default: not checked without `strict_mode` [emqx_frame.erl L713-716][f-utf8] | conform (D3) | unit |
| MQTT-3.1.3-5 | The server allows ClientIDs of 1 to 23 bytes from `0-9a-zA-Z` | session | conforms: any ClientID up to `max_clientid_len` bytes (65,535, never below 23) [emqx_packet.erl L332-365][p-clientid] [emqx_schema.erl L3623-3631][sc-clientid] | conform: up to 256 bytes of valid UTF-8 (O10) | session |
| MQTT-3.1.3-6 | A server that allows a zero-length ClientID assigns a unique one | session | conforms: assigns one with Clean Start 1 [emqx_channel.erl L2018-2027][c-assign-id]; with Clean Start 0 the CONNECT is refused with 0x85 [emqx_packet.erl L332-365][p-clientid] | conform: assigns one with either Clean Start (O9, O22) | session |
| MQTT-3.1.3-7 | The assigned ClientID is used as if the client had sent it and is returned as Assigned Client Identifier | session | conforms [emqx_channel.erl L2768-2778][c-assigned-prop] | conform | session |
| MQTT-3.1.3-8 | A rejected ClientID may get CONNACK 0x85; the server then closes | session | conforms | conform: a ClientID over 256 bytes gets CONNACK 0x85 (O10) | session |
| MQTT-3.1.3-9 | No will is sent when a new connection to the session arrives within the Will Delay | session | conforms: a takeover drops a delayed will [emqx_channel.erl L3029-3062][c-will-takeover] | conform: the claim at the log cancels a pending will (R3) | e2e |
| MQTT-3.1.3-10 | The order of the will's User Properties is kept when it is published | codec | conforms: user properties are kept as a list in order | conform: user properties decode to an ordered list, stored and sent as received | unit |
| MQTT-3.1.3-11 | The Will Topic is a UTF-8 Encoded String | codec | conforms: checked by `emqx_topic:validate` [emqx_packet.erl L386-404][p-will] | conform | unit |
| MQTT-3.1.3-12 | The User Name is a UTF-8 Encoded String | codec | deviates by default: not checked without `strict_mode` [emqx_frame.erl L713-716][f-utf8] | conform (D3) | unit |
| MQTT-3.1.4-1 | The server checks the CONNECT format and closes on a mismatch | codec | deviates by default: the checks behind `strict_mode` (UTF-8, header flags) are off and Maximum Packet Size 0 is not refused [emqx_packet.erl L367-384][p-connprops]; a malformed MQTT 5 CONNECT that is caught gets CONNACK 0x95, since every CONNECT parse error carries the protocol version [emqx_frame.erl L270-297][f-connect] [emqx_channel.erl L1246-1263][c-connect-0x95] | conform: CONNACK 0x81 for a malformed CONNECT, 0x82 for a protocol error such as Receive Maximum 0, then close (D3, D4) | unit |
| MQTT-3.1.4-2 | The server may check more and should authenticate and authorize; a failed check closes the connection | auth | conforms | conform: the edge runs bans and the `Authenticator`; a failure gets CONNACK 0x86, 0x87, 0x8A or 0x8C, then close | e2e |
| MQTT-3.1.4-3 | A CONNECT for a ClientID already connected sends that connection DISCONNECT 0x8E and closes it | edge | conforms [emqx_cm.erl L297-349][cm-open] | conform: the claim at the log makes the old edge step down and send 0x8E within a second (R2 rule 22, R3) | e2e |
| MQTT-3.1.4-4 | The server applies Clean Start | log | conforms | conform | e2e |
| MQTT-3.1.4-5 | An accepted CONNECT gets CONNACK 0x00 | session | conforms | conform: CONNACK goes out only after the claim commits (R3) | session |
| MQTT-3.1.4-6 | After refusing a CONNECT the server processes nothing the client sends but AUTH | session | conforms | conform: input after a refusing CONNACK is discarded | session |
| MQTT-3.2.0-1 | CONNACK 0x00 comes before any other server packet except AUTH | session | conforms | conform | session |
| MQTT-3.2.0-2 | At most one CONNACK per connection | session | conforms | conform | session |
| MQTT-3.2.2-1 | CONNACK flag bits 7 to 1 are 0 | codec | conforms | conform | unit |
| MQTT-3.2.2-2 | An accepted Clean Start 1 gets Session Present 0 | session | conforms | conform | session |
| MQTT-3.2.2-3 | An accepted Clean Start 0 gets Session Present 1 when the session existed, else 0 | session | conforms | conform: taken from the claim result (R3) | e2e |
| MQTT-3.2.2-4 | A client without session state that gets Session Present 1 closes the connection | client | client obligation | conform | unit |
| MQTT-3.2.2-5 | A client with session state that gets Session Present 0 discards it | client | client obligation | conform | unit |
| MQTT-3.2.2-6 | A CONNACK with a non-zero Reason Code has Session Present 0 | session | conforms [emqx_channel.erl L1320-1345][c-connack-error] | conform | session |
| MQTT-3.2.2-7 | After a CONNACK of 0x80 or above the server closes the connection | transport | conforms | conform: the transport closes once the CONNACK is delivered or a one second linger ends, because closing a QUIC connection discards stream data not yet acknowledged | e2e |
| MQTT-3.2.2-8 | CONNACK uses a defined Connect Reason Code | codec | conforms; the code chosen for a malformed MQTT 5 CONNECT is 0x95 [emqx_channel.erl L1246-1263][c-connect-0x95] | conform: reason codes are an enum per packet type, so an undefined one cannot be encoded (D4) | unit |
| MQTT-3.2.2-9 | A server without QoS 1 or 2 support sends Maximum QoS | session | conforms: sent when `max_qos_allowed` is below 2 [emqx_channel.erl L2694-2729][c-connack-caps] | conform: QoS 2 is supported, so Maximum QoS is sent only when an operator lowers it (O19) | session |
| MQTT-3.2.2-10 | Such a server still accepts SUBSCRIBE with any Requested QoS | session | conforms; it grants the requested QoS even above `max_qos_allowed` [emqx_channel.erl L954-969][c-subscribe-rc] | conform: the granted QoS is the requested one, capped at the server's Maximum QoS | session |
| MQTT-3.2.2-11 | A client publishes at no QoS above the Maximum QoS it was given | client | server side conforms: DISCONNECT 0x9B [emqx_mqtt_caps.erl L90-101][caps-pub] [emqx_channel.erl L680-690][c-pub-errors] | conform; the server answers DISCONNECT 0x9B | unit |
| MQTT-3.2.2-12 | A Will QoS above the server's capability is refused, should get CONNACK 0x9B, and closes | session | conforms [emqx_packet.erl L386-404][p-will] | conform | session |
| MQTT-3.2.2-13 | Will Retain 1 on a server without retained messages is refused, should get CONNACK 0x9A, and closes | session | conforms [emqx_packet.erl L386-404][p-will] | conform | session |
| MQTT-3.2.2-14 | A client told Retain Available 0 never publishes with RETAIN 1 | client | server side conforms: DISCONNECT 0x9A [emqx_mqtt_caps.erl L90-101][caps-pub] | conform; the server answers DISCONNECT 0x9A | unit |
| MQTT-3.2.2-15 | A client sends no packet above the server's Maximum Packet Size | client | server side conforms: DISCONNECT 0x95, but the limit is compared with the Remaining Length rather than the whole packet [emqx_frame.erl L202-219][f-remlen] [emqx_channel.erl L1233-1289][c-frame-error] | conform; the server measures the whole packet and answers DISCONNECT 0x95 (D23) | unit |
| MQTT-3.2.2-16 | A zero-length ClientID gets an Assigned Client Identifier that no other current session uses | session | conforms in practice: 16 random base62 characters, not checked for use [emqx_utils.erl L905-917][u-randid] | deviate on listeners configured for certificate identity, where the identifier is the CN and may name the device's own session (D20); elsewhere conform, with 23 characters carrying 125 random bits, checked by the claim (O9) | session |
| MQTT-3.2.2-17 | A client sends no Topic Alias above the server's Topic Alias Maximum | client | server side conforms: DISCONNECT 0x94 [emqx_channel.erl L2435-2451][c-alias-check] | conform; the server answers DISCONNECT 0x94 | unit |
| MQTT-3.2.2-18 | Without a server Topic Alias Maximum the client sends no Topic Alias | client | client obligation; the server always announces 65,535 [emqx_channel.erl L2694-2729][c-connack-caps] [emqx_schema.erl L3640-3647][sc-alias] | conform; the server announces 64 (O6) | unit |
| MQTT-3.2.2-19 | CONNACK leaves out a Reason String that would take it past the client's Maximum Packet Size | codec | conforms in practice: the only Reason String it sets is `THROTTLED` [emqx_channel.erl L594-619][c-post-connect]; a packet over the limit is dropped whole, not trimmed [emqx_frame.erl L751-761][f-serialize] | conform: the encoder drops the Reason String, then User Properties, until the packet fits | unit |
| MQTT-3.2.2-20 | CONNACK leaves out a User Property that would take it past the client's Maximum Packet Size | codec | conforms in practice: only hooks add User Properties; a packet over the limit is dropped whole [emqx_frame.erl L751-761][f-serialize] | conform | unit |
| MQTT-3.2.2-21 | A client given Server Keep Alive uses it instead of the Keep Alive it sent | client | client obligation | conform | unit |
| MQTT-3.2.2-22 | Without Server Keep Alive, the server uses the client's Keep Alive | session | conforms; `server_keepalive` (off by default) imposes one value on every client [emqx_channel.erl L2734-2740][c-server-ka] [emqx_schema.erl L3733-3740][sc-server-ka] | conform: Server Keep Alive is sent only when the client's value is 0 or outside the configured bounds (O4) | session |
| MQTT-3.3.1-1 | DUP is 1 when a PUBLISH is a re-delivery | session | conforms [emqx_session_mem.erl L748-759][m-replay] | conform | session |
| MQTT-3.3.1-2 | DUP is 0 on every QoS 0 PUBLISH | codec | conforms when sending; a received QoS 0 PUBLISH with DUP 1 passes without `strict_mode` [emqx_frame.erl L1161-1179][f-validate-header] | conform: received with DUP 1 it is a Malformed Packet (D3) | unit |
| MQTT-3.3.1-3 | The outgoing DUP depends only on whether that PUBLISH is a retransmission | session | conforms: DUP is cleared when a message is accepted [emqx_broker.erl L265-266][b-cleandup] | conform | session |
| MQTT-3.3.1-4 | A PUBLISH never has both QoS bits set | codec | conforms in closing; without `strict_mode` the reply is DISCONNECT 0x9B QoS not supported, not 0x81 [emqx_mqtt_caps.erl L90-101][caps-pub] | conform: Malformed Packet, DISCONNECT 0x81 (D4) | unit |
| MQTT-3.3.1-5 | RETAIN 1 from a client replaces the topic's retained message with this one | log | deviates: the store is skipped, with a warning and a normal PUBACK, above `retainer.max_payload_size` (1 MB) or `retainer.max_publish_rate` (1,000 a second) [emqx_retainer_publisher.erl L75-105][rp-store] [emqx_retainer_schema.erl L82-114][rs-limits], and for a new topic once the table is full [emqx_retainer_mnesia.erl L207-225][rm-full]; retained messages are held in RAM by default [emqx_retainer_schema.erl L138-143][rs-storage] | conform: the retained write commits before PUBACK (R3); a write the store refuses gets PUBACK or PUBREC 0x97 and the message is not routed (D15) | e2e |
| MQTT-3.3.1-6 | A zero-byte retained PUBLISH is delivered as usual and removes the topic's retained message | log | deviates under load: the removal shares the `max_publish_rate` limit and is skipped above it [emqx_retainer_publisher.erl L107-124][rp-delete]; `stop_publish_clear_msg` can also stop the delivery [emqx_retainer.erl L131-151][r-publish] | conform: the removal commits before PUBACK, with no rate limit (D15) | e2e |
| MQTT-3.3.1-7 | A zero-byte retained message is never stored | log | conforms [emqx_retainer.erl L131-151][r-publish] | conform | e2e |
| MQTT-3.3.1-8 | RETAIN 0 neither stores the message nor touches the retained one | log | conforms | conform | diff |
| MQTT-3.3.1-9 | Retain Handling 0 sends the retained messages matching the filter | session | conforms by default; dispatch is asynchronous, so a live message can arrive first [emqx_retainer_dispatcher.erl L60-62][rd-dispatch], and a delivery rate, when configured, drops the excess [emqx_retainer_dispatcher.erl L218-233][rd-batches] | conform: sent after the SUBACK and before any live message for the subscription (O2, D15) | e2e |
| MQTT-3.3.1-10 | Retain Handling 1 sends them only if the subscription did not exist | session | conforms [emqx_retainer.erl L121-129][r-subscribed] | conform | diff |
| MQTT-3.3.1-11 | Retain Handling 2 sends no retained messages | session | conforms [emqx_retainer.erl L121-129][r-subscribed]; Retain Handling 3 is not refused [emqx_frame.erl L653-657][f-subopts] | conform: Retain Handling 3 is a Protocol Error (D3) | diff |
| MQTT-3.3.1-12 | Retain As Published 0 forwards with RETAIN 0 | session | conforms [emqx_session.erl L519-524][s-rap] | conform: messages sent because a subscription was made keep RETAIN 1, as the spec says | diff |
| MQTT-3.3.1-13 | Retain As Published 1 forwards with the RETAIN flag as received | session | conforms [emqx_session.erl L519-524][s-rap] | conform | diff |
| MQTT-3.3.2-1 | The Topic Name comes first in the PUBLISH variable header and is a UTF-8 Encoded String | codec | conforms: checked by `emqx_topic:validate` [emqx_packet.erl L249-262][p-publish] | conform | unit |
| MQTT-3.3.2-2 | A Topic Name contains no wildcard | topic | conforms: such a PUBLISH gets DISCONNECT 0x90 [emqx_packet.erl L249-262][p-publish] [emqx_topic.erl L224-261][t-validate] | conform: such a PUBLISH gets PUBACK or PUBREC 0x90, or is dropped and counted at QoS 0 (O25, D32) | unit |
| MQTT-3.3.2-3 | The topic of a PUBLISH sent to a subscriber matches the subscription's filter | topic | conforms | conform: holds after the mountpoint is stripped (R2 rule 6) | unit |
| MQTT-3.3.2-4 | Payload Format Indicator reaches every subscriber unaltered | session | conforms [emqx_message.erl L354-366][g-props] | conform | diff |
| MQTT-3.3.2-5 | A copy whose Message Expiry Interval passed before delivery started is deleted | session | deviates in two cases: a retained message with interval 0 never expires [emqx_retainer.erl L169-182][r-expiry], and a copy delivered at once is not checked [emqx_session_mem.erl L525-550][m-deliver], so a message with interval 0 still reaches connected subscribers; queued copies expire as required [emqx_message.erl L289-302][g-expired] | conform: a copy expires at its deadline, the moment of receipt plus the interval, and is deleted if its delivery has not started by then; with interval 0 no copy is delivered, queued or retained (O8, D15) | session |
| MQTT-3.3.2-6 | A forwarded PUBLISH carries the received interval minus the time the message waited | session | deviates in one case: the seconds waited are rounded down and, once a millisecond has passed, the result is held at 1 or more, so a message received with interval 0 normally goes out with 1 [emqx_message.erl L304-320][g-update-expiry] | conform: the time left to the deadline, rounded up to whole seconds; an expired copy is never sent, so the value is never 0 (O8) | session |
| MQTT-3.3.2-7 | Topic Alias mappings never carry over to another connection | session | conforms | conform | session |
| MQTT-3.3.2-8 | A sender never uses Topic Alias 0 | session | conforms; a received alias 0 is refused [emqx_packet.erl L249-262][p-publish] [emqx_packet.erl L289-301][p-pubprops] | conform; a received alias 0 gets DISCONNECT 0x94 | session |
| MQTT-3.3.2-9 | A client sends no Topic Alias above the server's maximum | client | server side conforms: DISCONNECT 0x94 [emqx_channel.erl L2435-2451][c-alias-check] | conform | unit |
| MQTT-3.3.2-10 | A client accepts every alias from 1 to the maximum it announced | client | client obligation | conform | unit |
| MQTT-3.3.2-11 | The server sends no Topic Alias above the client's maximum | session | conforms [emqx_channel.erl L2380-2421][c-alias-out] | conform | session |
| MQTT-3.3.2-12 | The server accepts every alias from 1 to the maximum it announced | session | conforms [emqx_channel.erl L2346-2375][c-alias-in] [emqx_channel.erl L2435-2451][c-alias-check] | conform (O6); in multi-stream mode a Topic Alias travels on the control stream only (`docs/spec/mqtt-over-quic.md`, section 2.3) | session |
| MQTT-3.3.2-13 | The Response Topic is a UTF-8 Encoded String | codec | conforms: checked by `emqx_topic:validate` [emqx_packet.erl L289-301][p-pubprops] | conform | unit |
| MQTT-3.3.2-14 | The Response Topic contains no wildcard | topic | conforms: such a PUBLISH gets DISCONNECT 0x82 [emqx_packet.erl L289-301][p-pubprops] | conform: such a PUBLISH gets PUBACK or PUBREC 0x90, or is dropped and counted at QoS 0 (O25, D32) | unit |
| MQTT-3.3.2-15 | The Response Topic reaches every subscriber unaltered | session | conforms [emqx_message.erl L354-366][g-props] | conform: never mounted or stripped | diff |
| MQTT-3.3.2-16 | Correlation Data reaches every subscriber unaltered | session | conforms [emqx_message.erl L354-366][g-props] | conform | diff |
| MQTT-3.3.2-17 | Every User Property reaches the subscriber unaltered | session | conforms [emqx_message.erl L354-366][g-props] | conform | diff |
| MQTT-3.3.2-18 | User Properties keep their order when forwarded | codec | conforms | conform: an ordered list end to end, in storage and on the wire between roles too | unit |
| MQTT-3.3.2-19 | The Content Type is a UTF-8 Encoded String | codec | deviates by default: not checked without `strict_mode` [emqx_frame.erl L713-716][f-utf8] | conform (D3) | unit |
| MQTT-3.3.2-20 | Content Type reaches every subscriber unaltered | session | conforms [emqx_message.erl L354-366][g-props] | conform | diff |
| MQTT-3.3.4-1 | A PUBLISH gets the response its QoS calls for | session | conforms | conform | session |
| MQTT-3.3.4-2 | Overlapping subscriptions deliver the message at the highest QoS among them | edge | conforms: one copy per matching subscription, each at that subscription's QoS [emqx_broker.erl L752-778][b-dispatch] | conform: one copy per session, at the highest QoS (O11, D14) | e2e |
| MQTT-3.3.4-3 | The Subscription Identifiers of the matching subscriptions go with the message | edge | conforms [emqx_session.erl L525-527][s-subid] | conform | e2e |
| MQTT-3.3.4-4 | A single copy carries the identifiers of every matching subscription, in any order | edge | not exercised: there is always one copy per subscription | conform: this is the form used (O11) | e2e |
| MQTT-3.3.4-5 | With one copy per subscription, each carries its own subscription's identifier | edge | conforms [emqx_session.erl L525-527][s-subid] | conform: not exercised, one copy is sent (O11) | e2e |
| MQTT-3.3.4-6 | A client PUBLISH carries no Subscription Identifier | client | server side deviates: only the value 0 is refused [emqx_packet.erl L289-301][p-pubprops]; any other value stays on the message and reaches subscribers whose own subscription has none [emqx_message.erl L354-366][g-props] | conform: `openqtt-client` never sends one; the server answers one with DISCONNECT 0x82 (D12) | unit |
| MQTT-3.3.4-7 | A client keeps at most Receive Maximum QoS 1 and 2 PUBLISH packets unacknowledged | client | server side: Receive Maximum is announced as the client's own value capped at `max_inflight` (32) [emqx_channel.erl L2694-2729][c-connack-caps] [emqx_channel.erl L1924-1945][c-expiry], and only QoS 2 is held to a limit, `max_awaiting_rel` (100), with DISCONNECT 0x93 [emqx_session_mem.erl L369-386][m-qos2] [emqx_schema.erl L3839-3847][sc-await] | conform; the server announces 32 and holds QoS 1 and 2 to it with DISCONNECT 0x93 (O3, D13) | unit |
| MQTT-3.3.4-8 | A client whose quota is used up still sends packets other than PUBLISH | client | client obligation | conform | unit |
| MQTT-3.3.4-9 | The server keeps at most the client's Receive Maximum QoS 1 and 2 PUBLISH packets unacknowledged | session | conforms: the window is the client's value capped at `max_inflight` [emqx_session_mem.erl L525-550][m-deliver] [emqx_schema.erl L3893-3910][sc-inflight] | conform: capped at 32 (O3) | session |
| MQTT-3.3.4-10 | The server whose quota is used up still sends packets other than PUBLISH | session | conforms | conform | session |
| MQTT-3.4.2-1 | PUBACK uses a defined PUBACK Reason Code | codec | conforms | conform | unit |
| MQTT-3.4.2-2 | PUBACK leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.4.2-3 | PUBACK leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.5.2-1 | PUBREC uses a defined PUBREC Reason Code | codec | conforms | conform | unit |
| MQTT-3.5.2-2 | PUBREC leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.5.2-3 | PUBREC leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.6.1-1 | PUBREL fixed-header flags are 0010; any other value is malformed and closes the connection | codec | deviates by default: 0000 is taken as 0010 and other values pass without `strict_mode` [emqx_frame.erl L1241-1244][f-fixqos] [emqx_frame.erl L132-144][f-header] | conform (D3) | unit |
| MQTT-3.6.2-1 | PUBREL uses a defined PUBREL Reason Code | session | deviates: a repeated PUBREC gets PUBREL 0x91, which is not a PUBREL Reason Code [emqx_channel.erl L832-853][c-pubrec] [emqx_session_mem.erl L426-436][m-pubrec] | conform: a repeated PUBREC gets PUBREL 0x00 again; the codec cannot encode another code (D10) | session |
| MQTT-3.6.2-2 | PUBREL leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.6.2-3 | PUBREL leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.7.2-1 | PUBCOMP uses a defined PUBCOMP Reason Code | codec | conforms | conform | unit |
| MQTT-3.7.2-2 | PUBCOMP leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.7.2-3 | PUBCOMP leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.8.1-1 | SUBSCRIBE fixed-header flags are 0010; any other value is malformed and closes the connection | codec | deviates by default, as for PUBREL [emqx_frame.erl L1241-1244][f-fixqos] [emqx_frame.erl L132-144][f-header] | conform (D3) | unit |
| MQTT-3.8.3-1 | Topic Filters in SUBSCRIBE are UTF-8 Encoded Strings | codec | conforms: checked by `emqx_topic:validate` [emqx_topic.erl L224-261][t-validate] | conform | unit |
| MQTT-3.8.3-2 | SUBSCRIBE carries at least one Topic Filter and Subscription Options pair | codec | conforms in closing, with DISCONNECT 0x8F Topic Filter invalid [emqx_packet.erl L267-268][p-sub-empty] | conform: Protocol Error, DISCONNECT 0x82 (D4) | unit |
| MQTT-3.8.3-3 | No Local 1 means messages are not forwarded to a connection with the publisher's ClientID | session | conforms [emqx_session.erl L492-499][s-nolocal] | conform: compared on the ClientID, so a reconnect does not change it | diff |
| MQTT-3.8.3-4 | No Local 1 on a Shared Subscription is a Protocol Error | session | conforms: DISCONNECT 0x82 [emqx_packet.erl L416-429][p-sharenl] | conform | session |
| MQTT-3.8.3-5 | Non-zero reserved bits in the Subscription Options make the SUBSCRIBE malformed | codec | deviates: bits 6 and 7 are ignored, with or without `strict_mode` [emqx_frame.erl L653-657][f-subopts] | conform: Malformed Packet, DISCONNECT 0x81 (D3) | unit |
| MQTT-3.8.4-1 | Every SUBSCRIBE gets a SUBACK | session | conforms | conform | session |
| MQTT-3.8.4-2 | The SUBACK has the identifier of its SUBSCRIBE | session | conforms | conform | session |
| MQTT-3.8.4-3 | A Topic Filter identical to a non-shared subscription's replaces that subscription | session | conforms | conform | diff |
| MQTT-3.8.4-4 | On replacement, Retain Handling 0 resends retained messages, and no message is lost to the replacement | session | conforms | conform: the route entry is updated in place, never removed and re-added | e2e |
| MQTT-3.8.4-5 | Several filters in one SUBSCRIBE are handled as separate SUBSCRIBEs with one combined SUBACK | session | conforms | conform | session |
| MQTT-3.8.4-6 | The SUBACK has one Reason Code per filter | session | conforms | conform | session |
| MQTT-3.8.4-7 | Each Reason Code is the granted QoS or a failure | session | conforms: a denied filter gets 0x87 under the default `deny_action = ignore` [emqx_channel.erl L2541-2569][c-sub-deny]; the granted QoS is the requested one [emqx_channel.erl L954-969][c-subscribe-rc] | conform: a denied filter gets 0x87 (R2 rule 12, D2) | session |
| MQTT-3.8.4-8 | Messages go out at the lower of the published QoS and the granted QoS | session | conforms by default; `upgrade_qos = true` sends the higher one [emqx_session.erl L515-518][s-qos] [emqx_schema.erl L3911-3919][sc-upgrade] | conform: there is no upgrade setting (D21) | diff |
| MQTT-3.9.2-1 | SUBACK leaves out a Reason String that would take it past the client's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.9.2-2 | SUBACK leaves out a User Property that would take it past the client's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.9.3-1 | SUBACK Reason Codes are in the order of the SUBSCRIBE's filters | session | conforms | conform | session |
| MQTT-3.9.3-2 | SUBACK carries a defined Subscribe Reason Code for each filter | codec | conforms | conform | unit |
| MQTT-3.10.1-1 | UNSUBSCRIBE fixed-header flags are 0010; any other value is malformed and closes the connection | codec | deviates by default, as for PUBREL [emqx_frame.erl L1241-1244][f-fixqos] [emqx_frame.erl L132-144][f-header] | conform (D3) | unit |
| MQTT-3.10.3-1 | Topic Filters in UNSUBSCRIBE are UTF-8 Encoded Strings | codec | conforms: checked by `emqx_topic:validate` [emqx_topic.erl L224-261][t-validate] | conform | unit |
| MQTT-3.10.3-2 | UNSUBSCRIBE carries at least one Topic Filter | codec | conforms in closing, with DISCONNECT 0x8F [emqx_packet.erl L279-280][p-unsub-empty] | conform: Protocol Error, DISCONNECT 0x82 (D4) | unit |
| MQTT-3.10.4-1 | UNSUBSCRIBE filters are compared character by character, and an exact match deletes the subscription | session | deviates in one case: `$queue/t` and `$share/$queue/t` name the same subscription [emqx_topic.erl L372-406][t-parse] | conform: byte comparison and no aliases (D17) | session |
| MQTT-3.10.4-2 | After UNSUBSCRIBE no new matching messages are added for the client | session | conforms | conform | e2e |
| MQTT-3.10.4-3 | QoS 1 and 2 deliveries already started for those filters are completed | session | conforms | conform | session |
| MQTT-3.10.4-4 | Every UNSUBSCRIBE gets an UNSUBACK | session | conforms | conform | session |
| MQTT-3.10.4-5 | The UNSUBACK has the UNSUBSCRIBE's identifier, even when nothing was deleted | session | conforms | conform: a filter with no subscription gets 0x11 No subscription existed | session |
| MQTT-3.10.4-6 | Several filters in one UNSUBSCRIBE are handled as separate UNSUBSCRIBEs with one UNSUBACK | session | conforms | conform | session |
| MQTT-3.11.2-1 | UNSUBACK leaves out a Reason String that would take it past the client's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.11.2-2 | UNSUBACK leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.11.3-1 | UNSUBACK Reason Codes are in the order of the UNSUBSCRIBE's filters | session | conforms | conform | session |
| MQTT-3.11.3-2 | UNSUBACK carries a defined Unsubscribe Reason Code for each filter | codec | conforms | conform | unit |
| MQTT-3.12.4-1 | Every PINGREQ gets a PINGRESP | session | conforms | conform | session |
| MQTT-3.14.0-1 | The server sends no DISCONNECT before it has sent a CONNACK below 0x80 | session | deviates: a packet other than AUTH during enhanced authentication gets DISCONNECT 0x82 before any CONNACK [emqx_channel.erl L453-457][c-not-connected] [emqx_channel.erl L1389-1398][c-disconnect-out] | conform: before acceptance a refusal is a CONNACK, or a close without a reply (D4) | session |
| MQTT-3.14.1-1 | DISCONNECT reserved flag bits are 0; otherwise the receiver answers DISCONNECT 0x81 | codec | deviates by default: flags are not checked without `strict_mode` [emqx_frame.erl L132-144][f-header] | conform (D3) | unit |
| MQTT-3.14.2-1 | DISCONNECT uses a defined DISCONNECT Reason Code | codec | conforms when sending; a received undefined code passes [emqx_frame.erl L493-502][f-disconnect] | conform: a received undefined code is a Protocol Error (D3) | unit |
| MQTT-3.14.2-2 | The server never sends Session Expiry Interval on DISCONNECT | session | conforms | conform: the session never sets it on a DISCONNECT it sends | session |
| MQTT-3.14.2-3 | DISCONNECT leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms in practice: a packet over the limit is dropped whole [emqx_frame.erl L751-761][f-serialize] | conform: dropped first when the packet would not fit | unit |
| MQTT-3.14.2-4 | DISCONNECT leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms in practice, as above [emqx_frame.erl L751-761][f-serialize] | conform: dropped first when the packet would not fit | unit |
| MQTT-3.14.4-1 | Nothing is sent on the connection after DISCONNECT | session | conforms [emqx_channel.erl L1389-1398][c-disconnect-out] | conform | session |
| MQTT-3.14.4-2 | The sender of DISCONNECT closes the connection | transport | conforms | conform: closed once the DISCONNECT is delivered or a one second linger ends (MQTT-3.2.2-7) | e2e |
| MQTT-3.14.4-3 | DISCONNECT 0x00 from the client discards the will without publishing it | session | conforms [emqx_channel.erl L1062-1068][c-will-clean] | conform | diff |
| MQTT-3.15.1-1 | AUTH fixed-header flag bits are 0; any other value is malformed and closes the connection | codec | deviates by default: not checked without `strict_mode` [emqx_frame.erl L132-144][f-header] | conform (D3) | unit |
| MQTT-3.15.2-1 | AUTH uses a defined Authenticate Reason Code | codec | conforms; a received AUTH with an unexpected code is a Protocol Error [emqx_channel.erl L397-452][c-auth] | conform | unit |
| MQTT-3.15.2-2 | AUTH leaves out a Reason String that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |
| MQTT-3.15.2-3 | AUTH leaves out a User Property that would take it past the receiver's Maximum Packet Size | codec | conforms: it never sends one | conform: dropped first when the packet would not fit | unit |

## Table 4. Chapter 4, operational behaviour

Session state, the QoS flows, retries, ordering, topics, shared subscriptions, flow control, enhanced authentication and errors. Request and response (4.10) and server redirection (4.11) have no numbered statement; R3 covers redirection through `RedirectPolicy`.

| Statement | Requires | Layer | EMQX 5.8.9 | OpenQTT | Test |
| --- | --- | --- | --- | --- | --- |
| MQTT-4.1.0-1 | Session state is not discarded while the connection is open | session | deviates: a full queue (`max_mqueue_len`, 1,000) drops its oldest message and keeps the session [emqx_mqueue.erl L272-297][q-in] [emqx_schema.erl L3875-3892][sc-mqueue] | conform: a full queue ends the session, after DISCONNECT 0x97 closes the connection, so the client learns of the loss from Session Present 0 (O12, D16) | session |
| MQTT-4.1.0-2 | Session state is discarded once the connection is closed and the Session Expiry Interval has passed | log | conforms: a timer in the connection process [emqx_channel.erl L1772-1775][c-expire-session] | conform: expiry timers at the log partition delete the session and its queues (R3, O7) | e2e |
| MQTT-4.2.0-1 | Client and server support at least one transport giving an ordered, lossless byte stream each way | transport | conforms: TCP, TLS, WebSocket and QUIC listeners | conform: QUIC streams only (ADR 0002) | e2e |
| MQTT-4.3.1-1 | QoS 0 goes out as a PUBLISH with QoS 0 and DUP 0 | session | conforms | conform | session |
| MQTT-4.3.2-1 | QoS 1 takes an identifier not in use for each new message | session | deviates in the corner described at MQTT-2.2.1-4 [emqx_session_mem.erl L815-818][m-pktid] | conform (D7) | session |
| MQTT-4.3.2-2 | A QoS 1 PUBLISH first goes out with DUP 0 | session | conforms | conform | session |
| MQTT-4.3.2-3 | A QoS 1 PUBLISH stays unacknowledged until its PUBACK | session | conforms | conform | session |
| MQTT-4.3.2-4 | The QoS 1 receiver sends PUBACK with the identifier once it has taken ownership | session | conforms: PUBACK follows routing in memory, before other nodes have the message when forwarding is asynchronous | conform: PUBACK only after the retained write and every durable partition commit (R3, O24, D26) | e2e |
| MQTT-4.3.2-5 | After its PUBACK, a PUBLISH with the same identifier is a new message, whatever its DUP flag | session | conforms | conform | session |
| MQTT-4.3.3-1 | QoS 2 takes an identifier not in use for each new message | session | deviates in the corner described at MQTT-2.2.1-4 [emqx_session_mem.erl L815-818][m-pktid] | conform (D7) | session |
| MQTT-4.3.3-2 | A QoS 2 PUBLISH first goes out with DUP 0 | session | conforms | conform | session |
| MQTT-4.3.3-3 | A QoS 2 PUBLISH stays unacknowledged until its PUBREC | session | conforms | conform | session |
| MQTT-4.3.3-4 | A PUBREC below 0x80 is answered with a PUBREL carrying the same identifier | session | deviates: the PUBREC Reason Code is ignored, so a failure PUBREC (0x80 or above) also gets a PUBREL and the identifier stays in use [emqx_channel.erl L832-853][c-pubrec] [emqx_session_mem.erl L426-436][m-pubrec] | conform: a failure PUBREC ends the exchange and frees the identifier (D9) | session |
| MQTT-4.3.3-5 | A PUBREL stays unacknowledged until its PUBCOMP | session | conforms | conform | session |
| MQTT-4.3.3-6 | The PUBLISH is never resent once its PUBREL has gone out | session | conforms [emqx_session_mem.erl L748-759][m-replay] | conform | session |
| MQTT-4.3.3-7 | No message expiry is applied once the PUBLISH has been sent | session | conforms by default; with `retry_interval` set, an expired message still awaiting PUBREC is dropped [emqx_session_mem.erl L649-670][m-retry] [emqx_schema.erl L3768-3775][sc-retry] | conform: there is no retry setting (D21) | session |
| MQTT-4.3.3-8 | The QoS 2 receiver sends PUBREC with the identifier once it has taken ownership | session | conforms | conform: after the commit (R3) | e2e |
| MQTT-4.3.3-9 | After a PUBREC of 0x80 or above, a PUBLISH with that identifier is a new message | session | deviates after 0x91: the identifier stays awaiting PUBREL, so each later PUBLISH with it gets 0x91 again until `await_rel_timeout` (300 s) [emqx_session_mem.erl L369-386][m-qos2] [emqx_session_mem.erl L687-700][m-await-rel] [emqx_schema.erl L3920-3928][sc-await-rel] | conform (D8) | session |
| MQTT-4.3.3-10 | Until PUBREL, a PUBLISH repeating the identifier gets PUBREC again and is not delivered twice | session | deviates: a repeat gets PUBREC 0x91 Packet Identifier in use, which ends the exchange at a conforming client [emqx_session_mem.erl L369-386][m-qos2] [emqx_channel.erl L758-760][c-qos2-inuse]; after `await_rel_timeout` the identifier is forgotten and a repeat is delivered again [emqx_session_mem.erl L687-700][m-await-rel] | conform: PUBREC 0x00 again and no second delivery; the identifier is held in `rel/{cid}/{pid}` until PUBREL or the end of the session (D8) | session |
| MQTT-4.3.3-11 | A PUBREL is answered with a PUBCOMP carrying the same identifier | session | conforms | conform | session |
| MQTT-4.3.3-12 | After its PUBCOMP, a PUBLISH with the same identifier is a new message | session | conforms | conform | session |
| MQTT-4.3.3-13 | The QoS 2 receiver completes the exchange even after applying message expiry | session | conforms | conform | session |
| MQTT-4.4.0-1 | On reconnect with a session, unacknowledged PUBLISH and PUBREL packets are resent with their identifiers, and at no other time | session | conforms by default (`retry_interval` is infinity); resends go in identifier order, not send order [emqx_session_mem.erl L748-759][m-replay] [emqx_inflight.erl L119-121][i-tolist], and a set `retry_interval` resends within a connection [emqx_session_mem.erl L649-670][m-retry] [emqx_schema.erl L3768-3775][sc-retry] | conform: resent in the original order, and only when a session resumes on a new connection; a data stream ended while an exchange still needs a packet from the client closes the connection rather than moving the exchange to another stream (`docs/spec/mqtt-over-quic.md`, section 2.4); there is no retry setting (D11, D21) | session |
| MQTT-4.4.0-2 | A PUBACK or PUBREC of 0x80 or above acknowledges the PUBLISH, which is not retransmitted | session | conforms for the PUBLISH; a failure PUBREC still gets a PUBREL (MQTT-4.3.3-4) | conform (D9) | session |
| MQTT-4.5.0-1 | A message the server owns is added to the session state of every client with a matching subscription | router | conforms, within the queue limit of MQTT-4.1.0-1 | conform: a durable subscription belongs to a log partition, which receives an append (R3) | e2e |
| MQTT-4.5.0-2 | A client acknowledges every PUBLISH by its QoS, whether or not it processes the message | client | client obligation | conform | unit |
| MQTT-4.6.0-1 | A client resends PUBLISH packets in their original order | client | client obligation | conform | unit |
| MQTT-4.6.0-2 | A client sends PUBACK packets in the order the PUBLISH packets arrived | client | client obligation | conform; the server keeps the same order (O15) | unit |
| MQTT-4.6.0-3 | A client sends PUBREC packets in the order the PUBLISH packets arrived | client | client obligation | conform; the server keeps the same order (O15) | unit |
| MQTT-4.6.0-4 | A client sends PUBREL packets in the order the PUBREC packets arrived | client | client obligation | conform | unit |
| MQTT-4.6.0-5 | On an Ordered Topic the server sends one client's messages to consumers in the order received, per topic and QoS | edge | deviates in a corner: resends after a reconnect go in identifier order, which stops being the order received once identifiers wrap [emqx_session_mem.erl L748-759][m-replay] [emqx_inflight.erl L119-121][i-tolist]; live routing keeps order, and forwarding between nodes is keyed by topic | conform: one lane per publisher between edges and one append order per partition (R3, D11); in multi-stream mode the order holds per stream (`docs/spec/mqtt-over-quic.md`, section 2.3) | e2e |
| MQTT-4.6.0-6 | Every topic is an Ordered Topic for non-shared subscriptions | edge | conforms | conform | e2e |
| MQTT-4.7.0-1 | Wildcards may appear in Topic Filters, never in Topic Names | topic | conforms [emqx_topic.erl L224-261][t-validate] | conform: a Topic Name with one is refused as at MQTT-3.3.2-2 (O25) | unit |
| MQTT-4.7.1-1 | `#` stands alone or after `/`, and is the last character of the filter | topic | conforms: a filter that breaks it gets DISCONNECT 0x8F, for the whole SUBSCRIBE [emqx_topic.erl L224-261][t-validate] [emqx_packet.erl L267-287][p-sub-check] | conform: that filter alone gets 0x8F in the SUBACK or UNSUBACK (O25, D32) | unit |
| MQTT-4.7.1-2 | `+` occupies a whole level of the filter | topic | conforms, refusing as for `#` [emqx_topic.erl L224-261][t-validate] | conform: that filter alone gets 0x8F (O25) | unit |
| MQTT-4.7.2-1 | Filters starting with a wildcard do not match Topic Names starting with `$` | topic | conforms [emqx_topic.erl L83-103][t-match] [emqx_trie_search.erl L160-163][ts-dollar] | conform | unit |
| MQTT-4.7.3-1 | Topic Names and Topic Filters are at least one character long | topic | conforms [emqx_topic.erl L224-261][t-validate] | conform: an empty filter gets 0x8F (O25); an empty Topic Name without a Topic Alias is a Protocol Error, DISCONNECT 0x82 | unit |
| MQTT-4.7.3-2 | Topic Names and Topic Filters contain no U+0000 | topic | conforms [emqx_topic.erl L224-261][t-validate] | conform | unit |
| MQTT-4.7.3-3 | Topic Names and Topic Filters encode to at most 65,535 bytes | topic | conforms [emqx_topic.erl L224-261][t-validate] | conform | unit |
| MQTT-4.7.3-4 | Matching neither normalizes topics nor substitutes unrecognized characters | topic | conforms | conform: matching works on bytes; a mountpoint is a fixed prefix, not a normalization | unit |
| MQTT-4.8.2-1 | A Shared Subscription filter starts with `$share/` and has a ShareName of at least one character | topic | deviates: `$queue/t` is a shared subscription too, in the group `$queue` [emqx_topic.erl L372-406][t-parse]; `$share/` filters are checked as required [emqx_topic.erl L263-287][t-share] | conform: `$queue/` has no special meaning (D17), and a malformed `$share/` filter gets 0x8F (O25) | unit |
| MQTT-4.8.2-2 | The ShareName contains no `/`, `+` or `#`, and is followed by `/` and a Topic Filter | topic | conforms [emqx_topic.erl L263-287][t-share] | conform: a filter that breaks it gets 0x8F (O25) | unit |
| MQTT-4.8.2-3 | Each shared subscriber gets messages at its own granted QoS | router | conforms | conform (O1) | e2e |
| MQTT-4.8.2-4 | A QoS 2 shared delivery cut off by a disconnect is completed when that client reconnects | log | conforms: QoS 2 in flight stays with the session [emqx_session_mem.erl L781-806][m-redispatch] | conform | e2e |
| MQTT-4.8.2-5 | If that client's session ends first, the QoS 2 message goes to no other subscriber | log | conforms: a terminating session redispatches only QoS 1 in flight and its queue [emqx_session_mem.erl L781-806][m-redispatch] | conform | e2e |
| MQTT-4.8.2-6 | A PUBACK or PUBREC of 0x80 or above to a shared delivery discards the message, which goes to no other subscriber | router | conforms: acknowledgement codes are ignored, so nothing is redispatched [emqx_channel.erl L804-826][c-puback] | conform | e2e |
| MQTT-4.9.0-1 | The initial send quota is non-zero and at most the Receive Maximum | session | conforms | conform (O3) | session |
| MQTT-4.9.0-2 | At a send quota of zero, no further QoS 1 or 2 PUBLISH goes out | session | conforms [emqx_session_mem.erl L525-550][m-deliver] | conform | session |
| MQTT-4.9.0-3 | At a send quota of zero, every other packet is still processed and answered | session | conforms | conform | session |
| MQTT-4.12.0-1 | An unsupported Authentication Method may get CONNACK 0x8C or 0x87; the server then closes | auth | conforms | conform: methods come from `Authenticator` implementations; a method none of them claims gets CONNACK 0x8C | session |
| MQTT-4.12.0-2 | A server AUTH asking for more data carries 0x18 Continue authentication | session | conforms [emqx_channel.erl L397-452][c-auth] | conform | session |
| MQTT-4.12.0-3 | A client answers a server AUTH with an AUTH carrying 0x18 | client | server side conforms: any other code is a Protocol Error [emqx_channel.erl L397-452][c-auth] | conform | unit |
| MQTT-4.12.0-4 | The server may refuse the authentication at any step with CONNACK 0x80 or above, and closes | session | conforms | conform | session |
| MQTT-4.12.0-5 | With an Authentication Method in CONNECT, every AUTH and a successful CONNACK carry that same method | session | conforms | conform | session |
| MQTT-4.12.0-6 | Without one, the server sends no AUTH and no Authentication Method in CONNACK | session | conforms | conform | session |
| MQTT-4.12.0-7 | Without one, the client sends no AUTH | client | server side conforms: such an AUTH is refused [emqx_channel.erl L397-452][c-auth] | conform; the server answers DISCONNECT 0x82 | unit |
| MQTT-4.12.1-1 | Re-authentication uses the Authentication Method the connection was authenticated with | client | server side conforms: another method gets DISCONNECT 0x8C [emqx_channel.erl L397-452][c-auth] | conform | unit |
| MQTT-4.12.1-2 | A failed re-authentication closes the connection, after a DISCONNECT where possible | session | conforms [emqx_channel.erl L397-452][c-auth] | conform | session |
| MQTT-4.13.1-1 | A Malformed Packet or Protocol Error with a defined Reason Code closes the connection | session | conforms in closing; without `strict_mode` many malformed packets go undetected (rows above) | conform: CONNACK before acceptance, DISCONNECT after, then close (D3, D4) | session |
| MQTT-4.13.2-1 | A CONNACK or DISCONNECT of 0x80 or above means the connection closes, whether or not it was sent | transport | conforms | conform | e2e |

## Decisions that differ from EMQX

Every place where OpenQTT 2.0 chooses differently from EMQX 5.8.9, whether EMQX conforms there
or not. D1 settles the question ADR 0001 left open.

### D1. What a client of an older MQTT version receives

ADR 0001 refuses every protocol level but 5 and leaves the bytes to this report. The CONNECT's
protocol name and level decide them. The codec reads the fixed header, the protocol name and
the level of such a CONNECT and stops: it never decodes a 3.1 or 3.1.1 body.

| Protocol name | Protocol level byte | EMQX 5.8.9 | OpenQTT 2.0 |
| --- | --- | --- | --- |
| `MQTT` | 0x05 | accepted | accepted |
| `MQTT` or `MQIsdp` | 0x03 or 0x04 (MQTT 3.1, 3.1.1), or 0x83 or 0x84, the same with the bridge bit | accepted where the name fits the version (`MQIsdp` with 3, `MQTT` with 4), as a bridge when the bit is set; otherwise `20 02 00 01`, then close | `20 02 00 01`, then close |
| `MQTT` | 0x85, the bridge bit on version 5 | accepted as a bridge, since only the low four bits are the version [emqx_frame.erl L541-545][f-bridge] | `20 03 00 84 00`, then close; an MQTT 5 bridge uses No Local and Retain As Published instead |
| `MQTT` or `MQIsdp` | any other | read by its low four bits as above; a version other than 3, 4 or 5 gets `20 02 00 01` if the CONNECT parses in the 3.1.1 layout, and a close without a reply if not | `20 03 00 84 00`, then close |
| anything else | any | close without a reply | close without a reply |

`20 02 00 01` is a CONNACK with Remaining Length 2, acknowledge flags 0 and return code 0x01,
which MQTT 3.1 and 3.1.1 both define as "Connection Refused, unacceptable protocol version".
`20 03 00 84 00` is the MQTT 5 CONNACK: Remaining Length 3, flags 0, reason code 0x84
Unsupported Protocol Version and a Property Length of 0.

Why these bytes:

- MQTT-3.1.2-2 lets a server refuse a level other than 5 with or without a CONNACK; the one
  MUST is to close. MQTT-3.1.2-1 does the same for a name other than `MQTT`.
- A 3.1.1 client expects a CONNACK with a Remaining Length of 2 and a return code from 0 to 5.
  The MQTT 5 refusal has a Remaining Length of 3 and a code, 0x84, that 3.1.1 reserves: a
  strict client reports a malformed packet and a lenient one an unknown code. Neither tells
  its operator what is wrong.
- `20 02 00 01` is the refusal both older versions define for this very case, and the one
  MQTT 3.1.1's own MQTT-3.1.2-2 requires of a server that does not support the client's level.
  Every 3.1 and 3.1.1 client reports it as an unacceptable protocol version. It is also the
  code EMQX translates 0x84 into for those versions [emqx_reason_codes.erl L160-182][rc-compat].
- Closing without a reply looks like a network fault, and the client retries for ever.
- A level that is neither 3, 4 nor 5 has no older format to answer in. A later version of the
  protocol is likelier to read the newest format than an older one, so it gets the MQTT 5
  refusal.
- The bridge bit, 0x80 on the level, is a convention of 3.1 and 3.1.1 bridges that EMQX also
  reads. Levels 0x83 and 0x84 come from such bridges and get their version's refusal. MQTT 5
  gives every subscription No Local and Retain As Published, which do what the bit asked for,
  so 0x85 is simply a level other than 5.
- A name other than `MQTT` and `MQIsdp` is not MQTT. There is no format it could read, so the
  connection closes, as EMQX's does [emqx_frame.erl L1189-1200][f-protoname].

Over QUIC the refusal goes out on the control stream, and the connection closes once it has
been delivered (MQTT-3.2.2-7). ADR 0001's "CONNACK 0x84" stands for every client that reads the
MQTT 5 format; MQTT 3.1 and 3.1.1 clients receive the same refusal as their own code, 0x01.
When this report is accepted, `README.md` and the crate documentation of `openqtt-codec`, which
say CONNACK 0x84 for every other level, change to say this.

### D2 to D32

| | Where | EMQX 5.8.9 | OpenQTT 2.0, and why |
| --- | --- | --- | --- |
| D2 | A denied PUBLISH or SUBSCRIBE (MQTT-3.8.4-7, R2 rule 12) | With the default `deny_action = ignore`, a denied QoS 1 or 2 PUBLISH gets PUBACK or PUBREC 0x87, but a 3.1.1 PUBACK has no code, so a 3.1.1 publisher reads success; a denied filter gets SUBACK 0x87, or 0x80 for 3.1.1; a denied QoS 0 PUBLISH is dropped [emqx_channel.erl L643-661][c-pub-deny] [emqx_channel.erl L2541-2569][c-sub-deny]. `deny_action = disconnect` closes the connection instead | 0x87 in PUBACK, PUBREC and SUBACK every time, and no disconnect for a policy refusal; a denied QoS 0 PUBLISH is dropped and counted (O14). With MQTT 5 only, every acknowledgement can carry the refusal (ADR 0001) |
| D3 | Malformed input (MQTT-1.5.4-1, 1.5.4-2, 1.5.5-1, 1.5.7-1, 2.1.3-1, 2.2.1-3, 2.2.2-1, 3.1.3-4, 3.1.3-12, 3.3.1-2, 3.3.1-11, 3.3.2-19, 3.6.1-1, 3.8.1-1, 3.8.3-5, 3.10.1-1, 3.14.1-1, 3.14.2-1, 3.15.1-1) | `strict_mode` is off by default [emqx_schema.erl L3717-3724][sc-strict], so fixed-header flags, UTF-8 outside topics, identifier 0 and DUP on QoS 0 pass. In every mode, reserved Subscription Options bits [emqx_frame.erl L653-657][f-subopts], Retain Handling 3, longer Variable Byte Integers [emqx_frame.erl L642-651][f-vbi], properties not allowed in the packet type or repeated [emqx_frame.erl L567-640][f-props], Maximum Packet Size 0 [emqx_packet.erl L367-384][p-connprops] and undefined DISCONNECT codes [emqx_frame.erl L493-502][f-disconnect] pass | Always strict, with no setting: each is a Malformed Packet or a Protocol Error, as the standard classes it. A broker that accepts malformed input behaves as no other broker does, and the fuzzed codec has one grammar to hold. Clients moving from 1.x change protocol anyway (ADR 0001) |
| D4 | Reason codes for errors (MQTT-3.1.2-3, 3.1.2-9, 3.1.2-11, 3.1.2-30, 3.1.4-1, 3.3.1-4, 3.8.3-2, 3.10.3-2, 3.14.0-1) | A malformed MQTT 5 CONNECT gets CONNACK 0x95 Packet too large, since every CONNECT parse error carries the protocol version [emqx_frame.erl L270-297][f-connect] [emqx_channel.erl L1246-1263][c-connect-0x95]; a PUBLISH with both QoS bits set gets DISCONNECT 0x9B [emqx_mqtt_caps.erl L90-101][caps-pub]; an empty SUBSCRIBE or UNSUBSCRIBE gets DISCONNECT 0x8F [emqx_packet.erl L267-268][p-sub-empty]; a packet during enhanced authentication gets DISCONNECT 0x82 before any CONNACK [emqx_channel.erl L453-457][c-not-connected] | 0x81 for a malformed packet and 0x82 for a protocol error; before acceptance a refusal is a CONNACK, never a DISCONNECT. The reason code is the client's only diagnosis |
| D5 | Keep Alive (MQTT-3.1.2-22) | Sampled every `keepalive_check_interval`, so the close lands between 1.5 and 2 times Keep Alive [emqx_keepalive.erl L75-94][k-init] [emqx_keepalive.erl L138-158][k-check] | A deadline at 1.5 times Keep Alive, reset by every packet, and bounds on the value (O4). R2 rule 24 promises the close to within a second |
| D6 | Packets too large for the client (MQTT-3.1.2-24, 3.1.2-25) | Dropped by the connection after the message entered the window, where it keeps its slot until the session ends or a later connection raises the limit [emqx_connection.erl L879-891][conn-drop]; a packet exactly at the limit is dropped too [emqx_frame.erl L1151-1154][f-toolarge] | The session checks the size first and completes the message; a packet equal to the limit is sent |
| D7 | Packet identifiers (MQTT-2.2.1-4, 4.3.2-1, 4.3.3-1) | A wrapping counter; meeting an identifier still in flight crashes the connection [emqx_session_mem.erl L815-818][m-pktid] [emqx_inflight.erl L69-71][i-insert] | The next identifier not in flight |
| D8 | A repeated QoS 2 PUBLISH (MQTT-4.3.3-9, 4.3.3-10) | PUBREC 0x91, which a conforming client takes as a failure that ends the exchange; the identifier is kept for `await_rel_timeout` (300 s) and then forgotten, so a later repeat is delivered twice [emqx_session_mem.erl L369-386][m-qos2] [emqx_session_mem.erl L687-700][m-await-rel] | PUBREC 0x00 again and no second delivery; `rel/{cid}/{pid}` is kept until PUBREL or the end of the session, at most Receive Maximum entries |
| D9 | A failure PUBREC from the client (MQTT-4.3.3-4, 4.4.0-2) | The reason code is ignored and a PUBREL sent [emqx_channel.erl L832-853][c-pubrec] | The exchange ends and the identifier is free again, as section 2.2.1 says |
| D10 | A repeated PUBREC (MQTT-3.6.2-1) | PUBREL 0x91, which is not a PUBREL code [emqx_channel.erl L832-853][c-pubrec] | PUBREL 0x00 again |
| D11 | Resend order after a reconnect (MQTT-4.4.0-1, 4.6.0-5) | Identifier order, which stops being send order once identifiers wrap [emqx_session_mem.erl L748-759][m-replay] [emqx_inflight.erl L119-121][i-tolist] | The original send order |
| D12 | Subscription Identifiers (MQTT-3.3.4-6) | A client PUBLISH may carry one, and it is forwarded [emqx_packet.erl L289-301][p-pubprops] [emqx_message.erl L354-366][g-props]; a SUBSCRIBE with the largest legal value, 268,435,455, gets DISCONNECT 0xA1 [emqx_packet.erl L263-266][p-subid] | One on a client PUBLISH is a Protocol Error; every value from 1 to 268,435,455 is accepted |
| D13 | Receive Maximum (MQTT-3.3.4-7, 3.3.4-9) | Announced as the client's own value capped at 32; only QoS 2 is limited, at 100 [emqx_channel.erl L2694-2729][c-connack-caps] [emqx_session_mem.erl L369-386][m-qos2] | Announces its own limit, 32, and holds QoS 1 and 2 to it (O3) |
| D14 | Overlapping subscriptions (MQTT-3.3.4-2 to 3.3.4-5) | One copy per matching subscription [emqx_broker.erl L752-778][b-dispatch] | One copy per session (O11) |
| D15 | Retained messages (MQTT-3.3.1-5, 3.3.1-6, 3.3.1-9, 3.3.2-5) | Stores are skipped above a size, a rate or a table limit, with a normal PUBACK [emqx_retainer_publisher.erl L75-105][rp-store] [emqx_retainer_publisher.erl L107-124][rp-delete] [emqx_retainer_mnesia.erl L207-225][rm-full]; held in RAM by default [emqx_retainer_schema.erl L138-143][rs-storage]; interval 0 never expires [emqx_retainer.erl L169-182][r-expiry]; delivered by a worker pool after the subscription [emqx_retainer_dispatcher.erl L60-62][rd-dispatch] | The retained write is part of accepting the publish: committed before PUBACK, or refused with 0x97 and not routed. Durable (R2 rule 17). One expiry rule (O8). Delivered after the SUBACK and before live messages (O2) |
| D16 | Queues (MQTT-4.1.0-1) | A full queue drops its oldest message; QoS 0 is queued for offline sessions [emqx_mqueue.erl L272-297][q-in] [emqx_schema.erl L3875-3892][sc-mqueue] | A full queue ends the session; QoS 0 is not queued for offline sessions (O12, O13) |
| D17 | `$queue/` and `$exclusive/` filters (MQTT-4.8.2-1, 3.10.4-1) | `$queue/t` is the shared subscription `$share/$queue/t`, and `$exclusive/t` an exclusive subscription to `t` [emqx_topic.erl L372-406][t-parse] | Ordinary filters with no special meaning; `$share/` is the one shared form. Clients moving from 1.x subscribe with `$share/<group>/` (R10) |
| D18 | Session expiry (MQTT-3.1.2-23) | MQTT 5 values are used as sent, up to never [emqx_channel.erl L1924-1945][c-expiry] | Capped at 7 days by default and returned in CONNACK (O7) |
| D19 | A zero-length ClientID with Clean Start 0 (MQTT-3.1.3-6) | Refused with CONNACK 0x85 [emqx_packet.erl L332-365][p-clientid] | Accepted, with an assigned identifier (O22) |
| D20 | Certificate identity (MQTT-3.1.3-2, 3.2.2-16) | `peer_cert_as_clientid = cn` replaces the ClientID and tells the client nothing [emqx_channel.erl L2768-2778][c-assigned-prop] | On a listener configured for certificate identity, the subject CN names the session and is returned as Assigned Client Identifier, whatever ClientID was sent. That deviates from both statements: the CONNECT's ClientID does not name the session, and an empty one does not get a new session. A device must not be able to name another device's session (R2 rule 4), and the client is told the identifier its session is kept under |
| D21 | Settings not carried over (MQTT-3.3.1-6, 3.8.4-8, 4.3.3-7, 4.4.0-1) | `upgrade_qos`, `retry_interval`, `retainer.stop_publish_clear_msg` and `strict_mode` exist [emqx_schema.erl L3911-3919][sc-upgrade] [emqx_schema.erl L3768-3775][sc-retry] [emqx_retainer_schema.erl L82-114][rs-limits] [emqx_schema.erl L3717-3724][sc-strict] | None of them: switched on, the first three break the statements listed, and strict checking is always on (D3) |
| D22 | Topic aliases (MQTT-3.3.2-12) | Announces 65,535 [emqx_schema.erl L3640-3647][sc-alias] | Announces 64, and aliases travel on the control stream only (O6) |
| D23 | Maximum Packet Size (MQTT-3.2.2-15) | 1 MB, compared with the Remaining Length [emqx_frame.erl L202-219][f-remlen] | 1 MiB, compared with the whole packet (O5) |
| D24 | Shared subscriptions (MQTT-4.8.2-3) | Round robin per publishing connection by default, with six other strategies [emqx_shared_sub.erl L386-435][sh-pick] [emqx_schema.erl L3672-3687][sc-shared] | Round robin per publishing edge over connected members, or by publisher ClientID (O1) |
| D25 | ClientID format and length (MQTT-3.1.3-5, 3.2.2-16) | Assigned: 16 base62 characters [emqx_utils.erl L905-917][u-randid]. Accepted: up to 65,535 bytes [emqx_schema.erl L3623-3631][sc-clientid] | Assigned: 23 characters (O9). Accepted: up to 256 bytes (O10) |
| D26 | What an acknowledgement means (MQTT-4.3.2-4, 4.3.3-8) | PUBACK once the message is routed in memory | PUBACK once it is durable (R3, O24) |
| D27 | Limits (R2 rule 8) | A PUBLISH over `max_topic_levels` (128) gets DISCONNECT 0x90 [emqx_mqtt_caps.erl L90-101][caps-pub] [emqx_schema.erl L3632-3639][sc-levels]; subscriptions per session are unlimited [emqx_schema.erl L3893-3910][sc-inflight] | A PUBLISH over 128 levels gets PUBACK or PUBREC 0x90 and the connection stays; at most 1,000 subscriptions per session (O16) |
| D28 | Capabilities in CONNACK | Retain Available, Wildcard, Subscription Identifier and Shared Subscription Available are sent explicitly [emqx_channel.erl L2694-2729][c-connack-caps] | Every property whose absence means the same is left out (O19) |
| D29 | Wills when a node stops (MQTT-3.1.2-8) | The session ends with the node, so its will goes out at once, whatever the Will Delay [emqx_channel.erl L2998-3028][c-will-terminate] | The session outlives the edge; the will waits for its delay and is dropped if the client reconnects (O23) |
| D30 | Reason Strings | Only `THROTTLED`, on CONNACK [emqx_channel.erl L594-619][c-post-connect] | A fixed phrase on each refusal (O20) |
| D31 | A reconnect while the previous connection is still being cleaned up | Refused with CONNACK 0x89 and the Reason String `THROTTLED` [emqx_cm.erl L297-349][cm-open] [emqx_channel.erl L594-619][c-post-connect] | The claim at the log orders the two connections; the new one waits for it rather than being refused (R3) |
| D32 | Topic syntax errors (MQTT-3.3.2-2, 3.3.2-14, 4.7.0-1, 4.7.1-1, 4.7.1-2, 4.7.3-1, 4.8.2-1, 4.8.2-2) | A bad Topic Name gets DISCONNECT 0x90 and a bad Response Topic DISCONNECT 0x82 [emqx_packet.erl L249-262][p-publish] [emqx_packet.erl L289-301][p-pubprops]; one bad filter turns the whole SUBSCRIBE or UNSUBSCRIBE into DISCONNECT 0x8F [emqx_packet.erl L267-287][p-sub-check] | The one item is refused and the connection stays: PUBACK or PUBREC 0x90 for the PUBLISH, dropped and counted at QoS 0, and 0x8F in the SUBACK or UNSUBACK for that filter alone (O25) |

## Behaviour the specification leaves open

Where MQTT 5.0 lets a server choose, the choice OpenQTT makes, beside EMQX's. Numbers are
defaults; operators can change them unless a row says otherwise. R4 to R6 may move a number
once they have measurements, and say so.

| | Behaviour | MQTT 5.0 | EMQX 5.8.9 | OpenQTT 2.0 |
| --- | --- | --- | --- | --- |
| O1 | Which member of a shared subscription gets a message | Any member, chosen per message | `round_robin` per publishing connection by default, or `random`, `round_robin_per_group`, `sticky`, `local`, `hash_clientid`, `hash_topic` [emqx_schema.erl L3672-3687][sc-shared] [emqx_shared_sub.erl L386-435][sh-pick]; a member whose session is offline can be chosen and queues the message | Round robin per publishing edge over the members whose sessions are connected; a member with an offline session only when none is connected. One alternative, by the publisher's ClientID, keeps one publisher's messages on one member. R6 measures the route view this needs |
| O2 | Retained messages after a SUBSCRIBE: when, and in what order | Not specified | After the SUBACK, sent by a worker pool, so a live message can arrive first; in index order [emqx_retainer_dispatcher.erl L60-62][rd-dispatch] | After the SUBACK and before any live message for that subscription: the session holds live deliveries for it until the retained read completes. In byte order of topic, the log's key order (R3) |
| O3 | Receive Maximum, each way | Each side announces its own; absent means 65,535 | Announces the client's own value capped at `max_inflight` (32) and sends with the same window; inbound, QoS 2 is held to `max_awaiting_rel` (100) and QoS 1 not at all [emqx_channel.erl L1924-1945][c-expiry] [emqx_schema.erl L3893-3910][sc-inflight] [emqx_schema.erl L3839-3847][sc-await] | Announces 32 and holds inbound QoS 1 and 2 to it (DISCONNECT 0x93); sends with the client's value capped at 32. Thirty-two messages each way bound an edge to 32 MiB per connection and direction at the default Maximum Packet Size |
| O4 | Keep Alive bounds and Server Keep Alive | The client's value applies unless the server sends its own; 0 turns Keep Alive off | `server_keepalive` is off; when set it overrides every client [emqx_schema.erl L3733-3740][sc-server-ka] | Client values from 10 to 1,200 seconds are used as sent; 0 and values above 1,200 get Server Keep Alive 1,200; values below 10 get 10. Liveness bounds how long a dead client holds its session, its takeover and its will |
| O5 | Maximum Packet Size | Absent means the protocol's own limit | 1 MB, compared with the Remaining Length [emqx_schema.erl L3613-3622][sc-packet] [emqx_frame.erl L202-219][f-remlen] | 1 MiB (1,048,576 bytes) for the whole packet, announced in CONNACK (R2 rule 8) |
| O6 | Topic aliases | The server announces how many it accepts, and may send aliases up to the client's maximum and remap them | Announces 65,535 and keeps every inbound mapping; outbound, assigns an alias on a topic's first use up to the client's maximum and never remaps [emqx_schema.erl L3640-3647][sc-alias] [emqx_channel.erl L2380-2421][c-alias-out] | Announces 64. Aliases travel on the control stream only, in both directions (`docs/spec/mqtt-over-quic.md`, section 2.3): the server assigns them as EMQX does, up to 64 and never remapped, on the PUBLISH packets it sends there, and a Topic Alias on a data stream is a Protocol Error. MQTT 5 scopes a mapping to the connection and assumes one ordered stream, and across QUIC streams a PUBLISH could use an alias before the one that sets it arrives. 65,535 aliases of up to 65,535 bytes each is gigabytes per connection |
| O7 | A cap on Session Expiry Interval | From 0 to 0xFFFFFFFF, which means never; the server may answer with its own value | MQTT 5 values are used as sent [emqx_channel.erl L1924-1945][c-expiry]; `session_expiry_interval` (2 h) applies only to older clients [emqx_schema.erl L3821-3838][sc-session] | Capped at 7 days (604,800 s); CONNACK carries Session Expiry Interval whenever the value used differs, and a DISCONNECT that raises it is capped the same way |
| O8 | Message expiry, retained messages included | A copy not yet sent when its interval passes is deleted; absent means no expiry | Queued copies expire, checked to the millisecond [emqx_message.erl L289-302][g-expired]; a copy delivered at once is not checked [emqx_session_mem.erl L525-550][m-deliver], so a message with interval 0 reaches connected subscribers, normally with interval 1 [emqx_message.erl L304-320][g-update-expiry]; a retained message with interval 0 never expires [emqx_retainer.erl L169-182][r-expiry]; the store-wide defaults `message_expiry_interval` and `retainer.msg_expiry_interval` are off [emqx_schema.erl L3821-3838][sc-session] [emqx_retainer_schema.erl L47-53][rs-expiry] | One rule for every copy, retained ones included. A copy's deadline is the moment of receipt plus the interval, on the server's clock at full resolution; from its deadline on the copy is expired, and an expired copy whose delivery has not started is deleted, so an interval of 1 s is expired 1 s after receipt. Interval 0 puts the deadline at the moment of receipt: the message reaches no subscriber and is never queued, and a retained one replaces the topic's retained message and expires with it, leaving the topic with none. The PUBLISH is still acknowledged with 0x00. A copy that is sent carries the time left to its deadline in whole seconds, rounded up, so never 0. No store-wide default unless configured (R2 rule 20) |
| O9 | The Assigned Client Identifier | Unique among current sessions | 16 random base62 characters, the first a letter, not checked [emqx_utils.erl L905-917][u-randid] [emqx_channel.erl L2018-2027][c-assign-id] | `oq` and 21 random base62 characters: 23 characters from `0-9a-zA-Z`, which every MQTT server must accept (MQTT-3.1.3-5), carrying 125 random bits. The claim at the log refuses a collision and a new one is drawn |
| O10 | ClientID length and content | 1 to 23 bytes from `0-9a-zA-Z` must be accepted; more may be | Up to 65,535 bytes, content unchecked [emqx_schema.erl L3623-3631][sc-clientid] | Up to 256 bytes of valid UTF-8 without U+0000; longer gets CONNACK 0x85. The ClientID is a key in the log (`own/{cid}`), and R4 sizes keys |
| O11 | Overlapping subscriptions | One copy at the highest QoS, or one copy per subscription | One copy per subscription [emqx_broker.erl L752-778][b-dispatch] | One copy per session: the highest QoS among the matching subscriptions left after No Local, capped at the message's QoS; every Subscription Identifier among them; RETAIN as published if any of them has Retain As Published 1. A device then receives and acknowledges each message once |
| O12 | A session's queue limit, and what happens at it | An administrative matter; discarding state ends the session (section 4.1.1, non-normative) | 1,000 messages per session; the oldest is dropped and the session kept [emqx_schema.erl L3875-3892][sc-mqueue] [emqx_mqueue.erl L272-297][q-in] | 1,000 messages per session. Reaching it ends the session: DISCONNECT 0x97 first if the client is connected, then the session is discarded, so the client sees Session Present 0. A dropped QoS 1 message is a loss the client cannot detect; an ended session is one it can |
| O13 | QoS 0 messages for an offline session | Optional | Queued (`mqueue_store_qos0 = true`) [emqx_schema.erl L3875-3892][sc-mqueue] | Not queued: QoS 0 reaches connected sessions only. Queueing would cost a durable write per message for a QoS that promises nothing |
| O14 | A denied QoS 0 PUBLISH | There is no acknowledgement to refuse it in | Dropped, or DISCONNECT 0x87 with `deny_action = disconnect` [emqx_channel.erl L643-661][c-pub-deny] | Dropped and counted (`publish_denied`), and logged at a bounded rate. A disconnect would turn one misaddressed message into an outage of the device's subscriptions as well |
| O15 | The order of the server's PUBACK and PUBREC packets | Required of clients only (MQTT-4.6.0-2, 4.6.0-3) | Arrival order: each QoS 1 PUBLISH is answered as it is routed | Arrival order on each connection: an acknowledgement whose commit finished early waits for the earlier ones. Commits to different partitions finish out of order (R3), and arrival order is what every client of 1.x has seen |
| O16 | Topic levels, and subscriptions per session | No limit given | 128 levels, and a PUBLISH over it gets DISCONNECT 0x90 [emqx_schema.erl L3632-3639][sc-levels] [emqx_mqtt_caps.erl L90-101][caps-pub]; subscriptions unlimited [emqx_schema.erl L3893-3910][sc-inflight] | 128 levels (R2 rule 8): a PUBLISH over it gets PUBACK or PUBREC 0x90, or is dropped and counted at QoS 0, and a filter over it gets SUBACK 0x8F. 1,000 subscriptions per session; beyond that, SUBACK 0x97 |
| O17 | Control characters and non-characters in strings | Senders should avoid them; a receiver may treat them as malformed | Refused with `strict_mode` (U+0000 to U+001F and U+007F to U+009F), accepted otherwise [emqx_frame.erl L1246-1271][f-validate-utf8] | Accepted; only U+0000 and surrogates are refused (MQTT-1.5.4-1, 1.5.4-2). Refusing them is optional, and would turn away clients whose strings are only untidy |
| O18 | Response Information | Optional, when the client asks for it | A configured string, none by default [emqx_channel.erl L2745-2763][c-resp-info] | None |
| O19 | Capability properties in CONNACK | Absent Maximum QoS, Retain Available, Wildcard, Subscription Identifier and Shared Subscription Available mean supported | All sent explicitly [emqx_channel.erl L2694-2729][c-connack-caps] | All supported and all left out, unless an operator turns one off |
| O20 | Reason Strings | Optional and human-readable; not to be parsed | Only `THROTTLED`, on CONNACK [emqx_channel.erl L594-619][c-post-connect] | A fixed phrase on each refusal, naming the reason but no rule and no internal name; left out when Request Problem Information is 0 (MQTT-3.1.2-29) or when it would not fit |
| O21 | When a received QoS 2 message goes onward | On PUBLISH or on PUBREL | On PUBLISH [emqx_session_mem.erl L369-386][m-qos2] | Once the PUBLISH is committed, before PUBREL; `rel/{cid}/{pid}` prevents a second delivery (D8) |
| O22 | A zero-length ClientID with Clean Start 0 | Allowed where zero-length ClientIDs are | Refused with CONNACK 0x85 [emqx_packet.erl L332-365][p-clientid] | Accepted: the assigned identifier names a new session, which the client may resume later by sending it |
| O23 | Wills when the server closes the connection | Published after the Will Delay unless the client reconnects; only the client's DISCONNECT 0x00 removes the will | Published; when a node stops, at once whatever the delay, since the session ends with the node [emqx_channel.erl L2998-3028][c-will-terminate] | Published after the Will Delay unless the client reconnects within it. Sessions outlive an edge drain, so a client that reconnects within its delay publishes no will; devices that should not fire wills on a drain set a Will Delay of a few seconds |
| O24 | What PUBACK and PUBREC promise | That the receiver has taken ownership | The message has been routed in memory | The message is durable: the retained write and every durable partition commit are done (R3) |
| O25 | Topic syntax errors in a PUBLISH or a filter | These statements bind the sender and name no reason code, so MQTT-4.13.1-1 does not require a close | DISCONNECT: 0x90 for a Topic Name, 0x82 for a Response Topic, and 0x8F for a filter, which refuses the packet's other filters with it [emqx_packet.erl L249-262][p-publish] [emqx_packet.erl L289-301][p-pubprops] [emqx_packet.erl L267-287][p-sub-check] | The one item is refused and the connection stays: a PUBLISH whose Topic Name or Response Topic breaks section 4.7 gets PUBACK or PUBREC 0x90, or is dropped and counted at QoS 0; a filter that breaks sections 4.7 or 4.8 gets 0x8F in the SUBACK or UNSUBACK, alone. A device with one bad filter keeps its others and does not loop on reconnect. An empty Topic Name without a Topic Alias stays a Protocol Error |

## Summary

Counted from the rows above.

| Chapter | Statements | OpenQTT conform | OpenQTT deviate | EMQX deviates |
| --- | --- | --- | --- | --- |
| 1 Introduction | 5 | 5 | 0 | 3 |
| 2 Packet format | 7 | 7 | 0 | 2 |
| 3 Control packets | 175 | 173 | 2 | 21 |
| 4 Operational behaviour | 60 | 60 | 0 | 8 |
| All | 247 | 245 | 2 | 34 |

| Layer | Statements |
| --- | --- |
| session | 109 |
| codec | 70 |
| client | 25 |
| log | 14 |
| topic | 13 |
| edge | 7 |
| transport | 4 |
| router | 3 |
| auth | 2 |

| Test kind | Statements |
| --- | --- |
| unit | 106 |
| session | 88 |
| e2e | 37 |
| diff | 16 |

OpenQTT deviates from the specification in 2 statements, MQTT-3.1.3-2 and MQTT-3.2.2-16, both only on listeners configured for certificate identity (D20). It differs from EMQX in 32 decisions, D1 to D32, and settles 25 behaviours the specification leaves open, O1 to O25.

<!-- EMQX 5.8.9 permalinks, at tag emqx-v5.8.9 (upstream a8319fe2390169e1f2483e3ec80dd01a6cdb233d) -->
[b-cleandup]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_broker.erl#L265-L266
[b-dispatch]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_broker.erl#L752-L778
[c-alias-check]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2435-L2451
[c-alias-in]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2346-L2375
[c-alias-max]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2818-L2830
[c-alias-out]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2380-L2421
[c-assign-id]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2018-L2027
[c-assigned-prop]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2768-L2778
[c-auth]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L397-L452
[c-connack-caps]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2694-L2729
[c-connack-error]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1320-L1345
[c-connect]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L555-L592
[c-connect-0x95]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1246-L1263
[c-disconnect-out]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1389-L1398
[c-expire-session]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1772-L1775
[c-expiry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1924-L1945
[c-frame-error]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1233-L1289
[c-keepalive-timeout]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1724-L1735
[c-not-connected]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L453-L457
[c-post-connect]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L594-L619
[c-pub-deny]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L643-L661
[c-pub-errors]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L680-L690
[c-puback]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L804-L826
[c-pubrec]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L832-L853
[c-qos2-inuse]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L758-L760
[c-resp-info]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2745-L2763
[c-second-connect]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L380-L387
[c-server-ka]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2734-L2740
[c-sub-deny]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2541-L2569
[c-subscribe-rc]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L954-L969
[c-will-clean]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L1062-L1068
[c-will-delay]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L3063-L3081
[c-will-takeover]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L3029-L3062
[c-will-terminate]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_channel.erl#L2998-L3028
[caps-pub]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_mqtt_caps.erl#L90-L101
[cm-open]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_cm.erl#L297-L349
[conn-drop]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_connection.erl#L879-L891
[f-bridge]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L541-L545
[f-connect]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L270-L297
[f-disconnect]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L493-L502
[f-fixqos]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1241-L1244
[f-header]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L132-L144
[f-pair]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L664-L673
[f-props]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L567-L640
[f-protoname]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1189-L1200
[f-publish]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L394-L407
[f-remlen]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L202-L219
[f-reserved]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1202-L1214
[f-serialize]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L751-L761
[f-subopts]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L653-L657
[f-toolarge]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1151-L1154
[f-trailing]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L359-L367
[f-utf8]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L713-L716
[f-validate-header]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1161-L1179
[f-validate-utf8]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1246-L1271
[f-vbi]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L642-L651
[f-vbi-out]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_frame.erl#L1146-L1149
[g-expired]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_message.erl#L289-L302
[g-props]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_message.erl#L354-L366
[g-update-expiry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_message.erl#L304-L320
[i-insert]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_inflight.erl#L69-L71
[i-tolist]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_inflight.erl#L119-L121
[k-check]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_keepalive.erl#L138-L158
[k-init]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_keepalive.erl#L75-L94
[m-await-rel]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L687-L700
[m-deliver]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L525-L550
[m-pktid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L815-L818
[m-pubrec]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L426-L436
[m-qos2]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L369-L386
[m-redispatch]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L781-L806
[m-replay]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L748-L759
[m-retry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session_mem.erl#L649-L670
[p-clientid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L332-L365
[p-connprops]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L367-L384
[p-protover]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L320-L330
[p-publish]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L249-L262
[p-pubprops]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L289-L301
[p-sharenl]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L416-L429
[p-sub-check]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L267-L287
[p-sub-empty]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L267-L268
[p-subid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L263-L266
[p-unsub-empty]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L279-L280
[p-will]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_packet.erl#L386-L404
[q-in]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_mqueue.erl#L272-L297
[r-expiry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer.erl#L169-L182
[r-publish]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer.erl#L131-L151
[r-subscribed]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer.erl#L121-L129
[rc-compat]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_reason_codes.erl#L160-L182
[rd-batches]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_dispatcher.erl#L218-L233
[rd-dispatch]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_dispatcher.erl#L60-L62
[rm-full]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_mnesia.erl#L207-L225
[rp-delete]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_publisher.erl#L107-L124
[rp-store]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_publisher.erl#L75-L105
[rs-expiry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_schema.erl#L47-L53
[rs-limits]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_schema.erl#L82-L114
[rs-storage]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_retainer/src/emqx_retainer_schema.erl#L138-L143
[s-nolocal]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session.erl#L492-L499
[s-qos]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session.erl#L515-L518
[s-rap]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session.erl#L519-L524
[s-subid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_session.erl#L525-L527
[sc-alias]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3640-L3647
[sc-await]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3839-L3847
[sc-await-rel]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3920-L3928
[sc-clientid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3623-L3631
[sc-inflight]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3893-L3910
[sc-ka-check]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3760-L3767
[sc-levels]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3632-L3639
[sc-mqueue]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3875-L3892
[sc-packet]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3613-L3622
[sc-retry]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3768-L3775
[sc-server-ka]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3733-L3740
[sc-session]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3821-L3838
[sc-shared]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3672-L3687
[sc-strict]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3717-L3724
[sc-upgrade]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_schema.erl#L3911-L3919
[sh-pick]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_shared_sub.erl#L386-L435
[t-match]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L83-L103
[t-parse]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L372-L406
[t-share]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L263-L287
[t-validate]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L224-L261
[ts-dollar]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_trie_search.erl#L160-L163
[u-randid]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_utils/src/emqx_utils.erl#L905-L917
