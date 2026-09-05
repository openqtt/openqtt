# OpenQTT

OpenQTT is a maintained continuation of EMQX 5.8.9 under the Apache License 2.0.
It is an MQTT 3.1, 3.1.1 and 5.0 broker that clusters, with TLS, WebSocket and
mutual TLS listeners, a REST API, and an Erlang runtime.

- Source and issues: <https://github.com/openqtt/OpenQTT>
- Images: `ghcr.io/openqtt/openqtt`
- Architectures: `amd64`

## Running it

```console
docker run -d --name openqtt -p 1883:1883 -p 18083:18083 ghcr.io/openqtt/openqtt:1.0.0
```

The broker runs as Linux user `openqtt` (uid 1000) and is installed under
`/opt/openqtt`. There is no dashboard web UI: the REST API on 18083 is complete
and `/` answers 404. `openqtt ctl` and `openqtt` are on the PATH.

## Configuration

Everything in `etc/emqx.conf` can be set through environment variables prefixed
`OPENQTT_`:

	OPENQTT_DASHBOARD__DEFAULT_PASSWORD       <--> dashboard.default_password
	OPENQTT_NODE__COOKIE                      <--> node.cookie
	OPENQTT_LISTENERS__SSL__default__ENABLE   <--> listeners.ssl.default.enable

The lowercase `default` above is not a typo: case does not matter.

- The `OPENQTT_` prefix is removed
- Upper case becomes lower case
- `__` becomes `.`

```console
docker run -d --name openqtt \
  -e OPENQTT_DASHBOARD__DEFAULT_PASSWORD=mysecret \
  -p 1883:1883 -p 18083:18083 ghcr.io/openqtt/openqtt:1.0.0
```

There is no `EMQX_` fallback. An environment written for EMQX is read by
nothing, and the node starts on the config file's defaults instead.

The config keys themselves are unchanged from EMQX 5.8, so the [EMQX 5.8
configuration reference](https://docs.emqx.com/en/emqx/v5.8/configuration/configuration.html)
still describes them.

### Node name

`OPENQTT_NODE__NAME` sets the full node name, which defaults to
`<container_name>@<container_ip>`. Without it the node resolves its name from
the discovery settings below.

## Cluster

Set a static seed list, and give each node a name that survives a restart.

```yaml
services:
  node1:
    image: ghcr.io/openqtt/openqtt:1.0.0
    environment:
      - "OPENQTT_NODE__COOKIE=change-me"
      - "OPENQTT_NODE__NAME=openqtt@node1.openqtt.local"
      - "OPENQTT_CLUSTER__DISCOVERY_STRATEGY=static"
      - "OPENQTT_CLUSTER__STATIC__SEEDS=[openqtt@node1.openqtt.local, openqtt@node2.openqtt.local]"
    networks:
      openqtt:
        aliases: [node1.openqtt.local]

  node2:
    image: ghcr.io/openqtt/openqtt:1.0.0
    environment:
      - "OPENQTT_NODE__COOKIE=change-me"
      - "OPENQTT_NODE__NAME=openqtt@node2.openqtt.local"
      - "OPENQTT_CLUSTER__DISCOVERY_STRATEGY=static"
      - "OPENQTT_CLUSTER__STATIC__SEEDS=[openqtt@node1.openqtt.local, openqtt@node2.openqtt.local]"
    networks:
      openqtt:
        aliases: [node2.openqtt.local]

networks:
  openqtt:
    driver: bridge
```

`OPENQTT_NODE__COOKIE` is the Erlang distribution secret. Every node must share
it, and anything that reaches port 4370 knowing it gets code execution inside
the broker. The image falls back to `emqxsecretcookie`, which is published in
EMQX's git history, so set your own.

Check the cluster:

```console
docker exec -it node1 openqtt ctl cluster status
```

## Persistence

Keep these directories:

- `/opt/openqtt/data`
- `/opt/openqtt/log`

Data under `/opt/openqtt/data/mnesia/${node_name}` is per node name, so a node
that comes back under a different name comes back empty. Pin the host part of
`OPENQTT_NODE__NAME` to something stable: a container name, a hostname, or
`127.0.0.1` for a single node.

```yaml
volumes:
  openqtt-data:
  openqtt-log:

services:
  openqtt:
    image: ghcr.io/openqtt/openqtt:1.0.0
    restart: always
    environment:
      OPENQTT_NODE__NAME: openqtt@127.0.0.1
    volumes:
      - openqtt-data:/opt/openqtt/data
      - openqtt-log:/opt/openqtt/log
```

## Kernel tuning

File descriptor and socket limits are the usual first constraint. Pass them to
the container rather than mounting `/proc`:

```bash
docker run -d --name openqtt -p 1883:1883 -p 18083:18083 \
    --sysctl fs.file-max=2097152 \
    --sysctl fs.nr_open=2097152 \
    --sysctl net.core.somaxconn=32768 \
    --sysctl net.ipv4.tcp_max_syn_backlog=16384 \
    --sysctl net.core.netdev_max_backlog=16384 \
    --sysctl net.core.rmem_max=16777216 \
    --sysctl net.core.wmem_max=16777216 \
    ghcr.io/openqtt/openqtt:1.0.0
```

Do not run the container privileged to tune the kernel.

## Trademarks

EMQX is a trademark of EMQ Technologies Co., Ltd. OpenQTT is not affiliated
with, sponsored by, or endorsed by EMQ.
