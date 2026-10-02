# R2. The compatibility contract

Status: draft. Each numbered rule becomes one test in `crates/openqtt-compat`
(planned), runnable against the all-in-one binary, a kind cluster, and a deployed
cluster. Where a rule can also run against OpenQTT 1.0.x, the differential harness
runs it there too.

This contract describes the behaviour a fleet of devices with certificate identities
needs from the broker: identity from the certificate, a private topic namespace per
device, commands that wait for devices that are offline, and refusals the sender can
see. It is the 1.x deployment pattern, restated for MQTT 5 over QUIC, with the
places where 2.0 deliberately behaves differently marked **Changed**.

## Identity

1. **A device without a client certificate cannot connect.** The QUIC handshake
   fails with TLS alert 116 (certificate required).
2. **Only the device issuing CA is trusted.** A certificate that chains to the same
   root through another intermediate is refused. **Changed**: 1.x trusted the whole
   bundle it was given.
3. **The certificate must carry the clientAuth extended key usage.** A server
   certificate presented as a client certificate is refused. **Changed**.
4. **The subject CN is the username and the client identifier** on a listener
   configured for certificate identity. Username and password fields the client
   sends are ignored there. CNs may contain `/` and are at most 64 bytes.
5. **Password listeners** authenticate from a user list loaded at start (plain or
   hashed). A wrong or missing password gets CONNACK 0x86. Users are rotated
   through the admin API without a restart.

## Topic namespace

6. **The mountpoint** `ingest/${username}/` is prepended to every topic a device
   publishes and every filter it subscribes, and removed from every topic delivered
   to it. A device publishing `temperature` is seen by others at
   `ingest/<cn>/temperature`; a retained `ingest/<cn>/commands/firmware` reaches
   the device as `commands/firmware`.
7. **Authorization sees the topic before mounting.** Rules for devices are written
   against the device's own relative topics.
8. **Limits apply before mounting**: maximum packet size 1 MiB, maximum 128 topic
   levels. Over the limit, a publish gets DISCONNECT 0x95 or a refusal per the
   MQTT 5 rules, never a silent drop.

## Authorization

9. **Rules are evaluated in order, first match wins, and no match is a deny.**
10. **Rules can match** a username exactly or by regular expression, a client
    address by CIDR, a retain flag on publish, topics with `+` and `#`, and topics
    starting with `$` only when the rule names them (`#` never matches a
    `$`-topic).
11. **A subscribe filter is checked as a topic name against the rule's filter**, so
    a rule allowing `ingest/<org>/+/+/+` allows that filter and refuses broader
    ones.
12. **Refusals are visible.** A denied QoS 1 publish gets PUBACK 0x87, a denied
    subscription gets SUBACK 0x87, and a denied REST publish gets HTTP 403.
    **Changed**: 1.x acknowledged a denied publish as if it succeeded.
13. **A device can only receive commands, never send them.** A publish by a device
    to its own `commands/...` is refused, retained or not.
14. **The client address grants nothing by itself.** No rule gives a loopback or
    in-cluster address extra rights.
15. **Credential classes cannot be claimed by naming.** Rules for service
    credentials match exact names or a reserved prefix that the user list refuses
    to issue to anyone else.
16. **A device cannot set the retain flag.** A retained publish from a device gets
    PUBACK 0x87; the same publish without retain is accepted.

## Commands

17. **Retained messages are durable.** A retained message survives a full stop of
    every pod and is delivered unchanged afterwards.
18. **Retained messages are delivered right after CONNACK** to a device that
    subscribes on connect, before any live message on the same topic.
19. **A zero-byte retained publish clears the topic.**
20. **Retained messages do not expire** unless a message expiry interval is set on
    the publish or a store-wide limit is configured.
21. **The platform publishes commands over the admin REST API** (`POST
    /api/v1/publish` with `retain`, QoS 1, an API key). A 200 means the message is
    durable; a refusal is 403.

## Sessions

22. **A second connection with the same client identifier takes over.** The old
    connection gets DISCONNECT 0x8E within one second; the new one proceeds. This is
    how a device rotates its certificate without a gap.
23. **Clean start** with session expiry 0 keeps nothing across connections, so
    commands survive offline periods only as retained messages.
24. **Keep Alive** 30 seconds: a silent client is disconnected at 45 seconds, plus
    or minus one.
25. **A clean DISCONNECT** suppresses the will; an abrupt close publishes it.

## Transport

26. **MQTT 5 over QUIC** per `docs/spec/mqtt-over-quic.md`, single-stream mode,
    on UDP 443 or 14567.
27. **The client's real address is visible** to authorization and logs through
    the load balancer.
28. **A client that changes address keeps its connection** on a single edge
    (QUIC migration). Across edges, see R7.

## Operations

29. **Readiness** answers only when the node can serve; liveness never depends on
    another role.
30. **Configuration is converted, not reinterpreted.** `openqtt convert acl`
    turns a 1.x `acl.conf` into the 2.0 rule format and fails, with `--strict`,
    on any rule that conflicts with rules 13 to 16; the converted rules give the same
    decisions as the originals on the differential suite, except where this
    contract says **Changed**.
