# Configuration

Status: draft, part of report R9. Normative for OpenQTT 2.0; the configuration is frozen at
`v2.0.0-beta.1`.

An OpenQTT process reads its settings once, when it starts, in three layers, each over the one
before:

1. the built-in defaults, listed under [Settings](#settings);
2. a TOML file, named by `--config <path>`, or by the variable `OPENQTT_CONFIG` when the
   command line names none. Without either, no file is read: there is no default location;
3. `OPENQTT_` variables, one for each setting.

Nothing else is read. In particular `RUST_LOG` is not: the log filter is
`observability.log_level`.

## The file

TOML, with a table for each section. Every key is optional; a key the file leaves out keeps its
default. A listener for devices that are identified by their certificates:

```toml
[cluster]
roles = ["edge"]
node_name = "openqtt-edge-0"
seeds = ["openqtt-log:7000"]

[listeners.quic.devices]
bind = "0.0.0.0:443"
cert_file = "/etc/openqtt/tls/tls.crt"
key_file = "/etc/openqtt/tls/tls.key"
client_ca_file = "/etc/openqtt/tls/devices-ca.crt"
require_client_cert = true
identity_from_cn = true
enable_authn = false
mountpoint = "ingest/${username}/"

[observability]
log_format = "text"
```

A relative path is relative to the working directory, in the file and in a variable alike.

## Variables

A setting's variable is `OPENQTT_` and its key in capitals, with a double underscore between
levels: `limits.receive_maximum` is `OPENQTT_LIMITS__RECEIVE_MAXIMUM`, and
`listeners.quic.devices.bind` is `OPENQTT_LISTENERS__QUIC__DEVICES__BIND`. A single underscore
stays part of a name. The [Settings](#settings) tables give every variable.

| Type | In a variable |
| --- | --- |
| string, path, address, URL, duration | as in the file, without quotes |
| boolean | `true` or `false` |
| integer | decimal digits |
| size | bytes, or as in the file: `1MiB` |
| list | the items, separated by commas: `edge,router`. Empty, the list is empty |

An optional setting set to the empty string is unset. A variable can change a listener the file
or the defaults define but cannot create one, so a misspelt listener name in a variable is an
unknown variable, not a second listener.

`OPENQTT_CONFIG` names the file and is not a setting.

## Values

| Type | Written as |
| --- | --- |
| duration | a whole number and one unit, `ms`, `s`, `m`, `h` or `d`: `500ms`, `30s`, `20m`, `7d`. A bare number is refused, since nothing would say which unit it meant |
| size | bytes as an integer, or a whole number and a binary unit, `B`, `KiB`, `MiB` or `GiB`: `65536`, `64KiB`, `1MiB` |
| address | an IP address and a port: `0.0.0.0:14567`, `[::]:14567` |
| `host:port` | a host name or IP address and a port, an IPv6 address in brackets: `openqtt-log:7000` |
| URL | `http://` or `https://`, without credentials, query or fragment |
| path | a file or directory |
| path of a secret file | a file holding a secret; see [Secrets](#secrets) |

## Unknown names are errors

A key in the file that is not a setting, and an `OPENQTT_` variable that names none, stop the
process before it starts. The message names the nearest valid one:

```text
unknown key `listeners.quic.default.certfile` in /etc/openqtt/openqtt.toml; the nearest valid key is `listeners.quic.default.cert_file`
unknown variable OPENQTT_LOG; the nearest valid one is OPENQTT_OBSERVABILITY__LOG_LEVEL
```

OpenQTT 1.x ignored a variable that named nothing, so a stale or misspelt name changed nothing
and said nothing. 2.0 refuses it.

`OTEL_` variables are refused as well. The OpenTelemetry exporter in OpenQTT would read some of
them on its own, `OTEL_EXPORTER_OTLP_HEADERS` among them. OpenQTT sets every exporter setting
from `[observability.otlp]` and refuses the variables, so none can change the exporter behind
the configuration.

Every problem is reported at once, one per line, so a configuration is fixed in one pass.

## Secrets

No setting's value is a secret. A setting that needs one, a private key or a token, names the
file that holds it, and its key ends in `_file`. The file is read when the secret is needed. So a
secret never appears in the configuration file, in a variable, in the printed settings, or in an
error about the configuration. The value of an unknown key or variable is never repeated, in
case it is a secret set by mistake, and a URL carrying a password is refused without repeating
it.

## Checking and printing

Every command reads the same sources, `--config` included, which may come before or after the
subcommand.

- `openqtt config check` loads the settings, applies the rules between them, checks that every
  file a `*_file` setting names can be opened and that `observability.log_level` parses, and
  reports every problem, one per line. It exits with status 0 when there is none, and 2
  otherwise.
- `openqtt config print` writes, as TOML, what the file and the variables set.
- `openqtt config print --effective` writes every setting, defaults included, in the order of
  the tables below, with an unset setting as a comment. The text loads back to the same
  settings, so it can serve as a file.

`openqtt run` exits with status 2, before it starts anything, on any problem with the settings
themselves; the files they name are read by the parts of the broker that use them.

## Logs

A process writes its logs to stderr. In the default format, `json`, each line is one object:
`timestamp`, `level`, `message` and `target`, the event's own fields beside them, and `spans`,
the spans the event was written in with their fields, among them `client_id`, `partition` and
`node`. The format `text` writes the same as plain lines, for a person at a terminal.

## Settings

<!-- BEGIN SETTINGS: written by `make config-doc` from crates/config/src/settings.rs -->

### `[cluster]`

This node, the cluster it belongs to, and the roles it runs (R3).

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `name` | string | `"openqtt"` | `OPENQTT_CLUSTER__NAME` | The cluster's name. Every certificate between roles carries it, in `spiffe://openqtt/<cluster>/<role>/<node>` (R3), so it is lowercase letters, digits and `-`, at most 63 characters. |
| `node_name` | string | unset | `OPENQTT_CLUSTER__NODE_NAME` | This node's name, unique in the cluster: in Kubernetes, the pod's name. Required unless `roles` is `["all"]`. Lowercase letters, digits, `-` and `.`, at most 253 characters. |
| `roles` | list, each one of `all`, `edge`, `router`, `log`, `admin` | `["all"]` | `OPENQTT_CLUSTER__ROLES` | The roles this process runs: `all`, or one or more of `edge`, `router`, `log` and `admin` (R3). |
| `seeds` | list of `host:port` | `[]` | `OPENQTT_CLUSTER__SEEDS` | Where this node finds the others when it starts. In Kubernetes, the headless Service of each role (R3). |
| `zone` | string | unset | `OPENQTT_CLUSTER__ZONE` | The failure zone this node runs in. The log spreads the replicas of a partition across zones (R3). Unset, the node is in no zone. |
| `cert_file` | path | unset | `OPENQTT_CLUSTER__CERT_FILE` | The certificate chain (PEM) this node presents to the others, naming the cluster, its role and itself (R3). |
| `key_file` | path of a secret file | unset | `OPENQTT_CLUSTER__KEY_FILE` | The private key (PEM) of `cert_file`. |
| `ca_file` | path | unset | `OPENQTT_CLUSTER__CA_FILE` | The CA certificates (PEM) the other nodes' certificates must chain to. |

### `[listeners]`

The listeners clients connect to. Only the edge role opens them.

### `[listeners.quic.<name>]`

MQTT 5 over QUIC listeners (docs/spec/mqtt-over-quic.md), each named by the operator. There is one by default, named `default`; a file that names any replaces it. A variable can change a listener that exists but never create one, so a misspelt name is an error rather than a second listener. Two things have no setting: 0-RTT stays off until report R7 has measured where resuming clients land (R7, D5), and QUIC datagrams are off because MQTT over QUIC does not use them (spec section 1).

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `bind` | address, `ip:port` | `"0.0.0.0:14567"` | `OPENQTT_LISTENERS__QUIC__<NAME>__BIND` | The UDP address to listen on. 14567 is the port EMQX uses for MQTT over QUIC; a deployment facing the internet should also listen on 443 (spec section 1). |
| `cert_file` | path | unset | `OPENQTT_LISTENERS__QUIC__<NAME>__CERT_FILE` | The server's certificate chain (PEM). Required where the edge role runs. |
| `key_file` | path of a secret file | unset | `OPENQTT_LISTENERS__QUIC__<NAME>__KEY_FILE` | The private key (PEM) of `cert_file`. Required where the edge role runs. |
| `client_ca_file` | path | unset | `OPENQTT_LISTENERS__QUIC__<NAME>__CLIENT_CA_FILE` | The CA certificates (PEM) a client certificate must chain to. Only these are trusted for clients, not the roots of `cert_file` (R2 rule 2). |
| `require_client_cert` | boolean | `false` | `OPENQTT_LISTENERS__QUIC__<NAME>__REQUIRE_CLIENT_CERT` | Refuse a client that presents no certificate (R2 rule 1). Needs `client_ca_file`. |
| `identity_from_cn` | boolean | `false` | `OPENQTT_LISTENERS__QUIC__<NAME>__IDENTITY_FROM_CN` | Name each client by its certificate: the subject CN becomes its username and its client identifier, whatever the client sends (R2 rule 4, R1 D20). Needs `require_client_cert`, and `enable_authn` off: the certificate is the authentication. |
| `mountpoint` | string | `""` | `OPENQTT_LISTENERS__QUIC__<NAME>__MOUNTPOINT` | A prefix added to every topic a client of this listener publishes and subscribes to, and removed from every topic delivered to it (R2 rule 6). `${username}` and `${clientid}` stand for the client's own. Empty, topics are left as they are. |
| `enable_authn` | boolean | `true` | `OPENQTT_LISTENERS__QUIC__<NAME>__ENABLE_AUTHN` | Authenticate each client with the user list of `[auth]` (R2 rule 5). Off, a client connects without a password: for development, or with `identity_from_cn`. |
| `max_streams` | integer | `8` | `OPENQTT_LISTENERS__QUIC__<NAME>__MAX_STREAMS` | The bidirectional streams a client may open, the control stream included (R7, D2): the control stream and seven data streams. quinn keeps about 40 bytes of state for each from the moment a connection starts. A client may open no unidirectional streams. |
| `stream_window` | size | `"1MiB"` | `OPENQTT_LISTENERS__QUIC__<NAME>__STREAM_WINDOW` | The receive window of each stream (R7, D2). |
| `connection_window` | size | `"1MiB"` | `OPENQTT_LISTENERS__QUIC__<NAME>__CONNECTION_WINDOW` | The receive window of the whole connection (R7, D2). |
| `mtu_discovery` | boolean | `true` | `OPENQTT_LISTENERS__QUIC__<NAME>__MTU_DISCOVERY` | Probe each path for an MTU above QUIC's 1,200 bytes (R7, D2). |
| `session_tickets` | boolean | `true` | `OPENQTT_LISTENERS__QUIC__<NAME>__SESSION_TICKETS` | Let clients resume TLS sessions from stateless tickets (R7, D5). Off, every handshake is a full one. |

### `[limits]`

What an edge allows each client, from the choices report R1 makes where MQTT 5.0 leaves a value open (its O-entries).

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `keep_alive_min` | duration | `"10s"` | `OPENQTT_LIMITS__KEEP_ALIVE_MIN` | The shortest Keep Alive a client is given. A client asking for less, other than 0, gets this value as Server Keep Alive (R1, O4). |
| `keep_alive_max` | duration | `"20m"` | `OPENQTT_LIMITS__KEEP_ALIVE_MAX` | The longest Keep Alive a client is given. A client asking for more, or for 0, gets this value as Server Keep Alive (R1, O4). The QUIC idle timeout is set above 1.5 times it (spec section 6). |
| `receive_maximum` | integer | `32` | `OPENQTT_LIMITS__RECEIVE_MAXIMUM` | The Receive Maximum announced in CONNACK, which inbound QoS 1 and 2 messages are held to; messages to a client are held to the lower of it and the client's own (R1, O3). |
| `maximum_packet_size` | size | `"1MiB"` | `OPENQTT_LIMITS__MAXIMUM_PACKET_SIZE` | The Maximum Packet Size announced in CONNACK, counted over the whole packet (R1, O5, R2 rule 8). |
| `topic_alias_maximum` | integer | `64` | `OPENQTT_LIMITS__TOPIC_ALIAS_MAXIMUM` | The Topic Alias Maximum announced in CONNACK; 0 accepts no aliases (R1, O6). |
| `session_expiry_max` | duration | `"7d"` | `OPENQTT_LIMITS__SESSION_EXPIRY_MAX` | The longest Session Expiry Interval a session is kept for. A longer one, never included, is cut to this and returned in CONNACK (R1, O7). |
| `max_subscriptions` | integer | `1000` | `OPENQTT_LIMITS__MAX_SUBSCRIPTIONS` | The subscriptions one session may hold; one more gets SUBACK 0x97 (R1, O16). |
| `max_topic_levels` | integer | `128` | `OPENQTT_LIMITS__MAX_TOPIC_LEVELS` | The levels a topic or filter may have. A PUBLISH with more gets 0x90 and a filter with more 0x8F (R1, O16, R2 rule 8). |
| `max_queued_messages` | integer | `1000` | `OPENQTT_LIMITS__MAX_QUEUED_MESSAGES` | The messages one session may have queued. Reaching it ends the session (R1, O12). |
| `max_client_id_length` | integer | `256` | `OPENQTT_LIMITS__MAX_CLIENT_ID_LENGTH` | The longest Client Identifier accepted, in bytes; a longer one gets CONNACK 0x85 (R1, O10). At least 23, which MQTT requires every server to accept. |

### `[auth]`

How clients are authenticated and authorized.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `password_bootstrap_file` | path of a secret file | unset | `OPENQTT_AUTH__PASSWORD_BOOTSTRAP_FILE` | Users to load when the cluster starts, for listeners with `enable_authn` (R2 rule 5). The admin API manages them after that. |
| `password_bootstrap_type` | one of `plain`, `hashed` | `"hashed"` | `OPENQTT_AUTH__PASSWORD_BOOTSTRAP_TYPE` | How `password_bootstrap_file` holds passwords: `plain`, hashed when loaded, or already `hashed`. |
| `acl_file` | path | unset | `OPENQTT_AUTH__ACL_FILE` | The authorization rules, evaluated in order, the first match deciding (R2 rules 9 to 11). Unset, no rule matches, so every publish and subscription is denied. |

### `[auth.jwt]`

Authentication by JSON Web Token. Not available yet.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `OPENQTT_AUTH__JWT__ENABLED` | Must stay `false`: JWT authentication is not built yet, and turning it on is refused rather than ignored. |

### `[auth.http]`

Authentication by an HTTP service. Not available yet.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `OPENQTT_AUTH__HTTP__ENABLED` | Must stay `false`: HTTP authentication is not built yet, and turning it on is refused rather than ignored. |

### `[edge]`

The edge role: client connections and the interest it registers with the routers.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `interest_threshold` | integer | `64` | `OPENQTT_EDGE__INTEREST_THRESHOLD` | T in report R6: below a node of an edge's interest, a level with more than this many distinct values becomes `+`, and the resulting shape is registered once more than this many filters share it (R6, D2 and D3). |
| `interest_floor` | integer | `1` | `OPENQTT_EDGE__INTEREST_FLOOR` | The shallowest depth at which interest is coarsened: the root is 0 and the first level 1 (R6, D3). A deployment that places connections by namespace raises it. |
| `interest_grace` | duration | `"30s"` | `OPENQTT_EDGE__INTEREST_GRACE` | How long an edge keeps a departed client's interest before withdrawing it, so that a client reconnecting to the same edge changes no route (R6, D5). |

### `[router]`

The router role: the interest index.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `shards` | integer | `64` | `OPENQTT_ROUTER__SHARDS` | The virtual shards of the interest index, assigned to the live routers by rendezvous hashing (R3). |

### `[log]`

The log role: durable state in Raft partitions.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `partitions` | integer | `256` | `OPENQTT_LOG__PARTITIONS` | The log's partitions, each a Raft group. Fixed when the cluster is created (R3). |
| `replication_factor` | integer | `1` | `OPENQTT_LOG__REPLICATION_FACTOR` | The replicas of each partition: 1, or 3 across zones (R3). |

### `[admin]`

The admin role: the REST API.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `api_key_bootstrap_file` | path of a secret file | unset | `OPENQTT_ADMIN__API_KEY_BOOTSTRAP_FILE` | API keys to load when the cluster starts. The admin API manages them after that. |

### `[http]`

The HTTP listener every process runs, whatever its roles.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `bind` | address, `ip:port` | `"0.0.0.0:8080"` | `OPENQTT_HTTP__BIND` | The TCP address of the HTTP listener every process runs: `/healthz` and `/readyz` on every role, `/metrics` when `observability.prometheus.enabled`, and the admin API where the admin role runs. Keep it inside the cluster (R3). |

### `[observability]`

Logs and metrics.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `log_level` | string | `"info"` | `OPENQTT_OBSERVABILITY__LOG_LEVEL` | Which log events are written: a level (`error`, `warn`, `info`, `debug`, `trace`), or directives such as `info,openqtt_edge=debug`. |
| `log_format` | one of `json`, `text` | `"json"` | `OPENQTT_OBSERVABILITY__LOG_FORMAT` | How log lines are written to stderr: `json`, one object per line, or `text`. |

### `[observability.otlp]`

Pushing metrics to an OpenTelemetry collector.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `endpoint` | URL | unset | `OPENQTT_OBSERVABILITY__OTLP__ENDPOINT` | The collector's base URL, for OTLP over HTTP with protobuf bodies: metrics go to its `/v1/metrics`. Unset, nothing is pushed. |
| `headers_file` | path of a secret file | unset | `OPENQTT_OBSERVABILITY__OTLP__HEADERS_FILE` | HTTP headers to send with every export, one `name: value` per line, for a collector that wants a token. |
| `ca_file` | path | unset | `OPENQTT_OBSERVABILITY__OTLP__CA_FILE` | The CA certificates (PEM) an `https` endpoint's certificate must chain to. Unset, the Mozilla roots built into OpenQTT are trusted. |
| `interval` | duration | `"1m"` | `OPENQTT_OBSERVABILITY__OTLP__INTERVAL` | How often metrics are pushed. |
| `timeout` | duration | `"10s"` | `OPENQTT_OBSERVABILITY__OTLP__TIMEOUT` | How long one push may take. |

### `[observability.prometheus]`

Metrics for Prometheus to scrape.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `false` | `OPENQTT_OBSERVABILITY__PROMETHEUS__ENABLED` | Serve metrics at `/metrics` on the HTTP listener, for a scraper that presents the token in `token_file`. |
| `token_file` | path of a secret file | unset | `OPENQTT_OBSERVABILITY__PROMETHEUS__TOKEN_FILE` | The bearer token a scraper must present. Required when `enabled`: metrics are never served to anyone who asks. |

### `[storage]`

Where the process keeps state on disk.

| Key | Type | Default | Variable | Description |
| --- | --- | --- | --- | --- |
| `data_dir` | path | `"/var/lib/openqtt"` | `OPENQTT_STORAGE__DATA_DIR` | The directory the process keeps its state in. The log role's partitions live here; report R4 decides the engine and its settings. |

<!-- END SETTINGS -->
