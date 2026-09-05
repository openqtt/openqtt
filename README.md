# OpenQTT

OpenQTT is a maintained continuation of EMQX 5.8.9 under the Apache License 2.0.

EMQX moved to the Business Source License at 5.9 and, from that version on, a
cluster of more than one node needs a paid license. The Apache 2.0 line, 5.8,
reached end of life on 28 February 2026. This repository begins where that line
stopped: the Apache-licensed source of EMQX v5.8.9, upstream commit
`a8319fe2390169e1f2483e3ec80dd01a6cdb233d`, imported as one commit and tagged
`emqx-v5.8.9`.

It is an MQTT 3.1, 3.1.1 and 5.0 broker that clusters, with TLS, WebSocket and
mutual TLS listeners, a REST API, and an Erlang runtime. Nothing about how it
speaks MQTT has changed.

## What is here, and what is not

Everything in this tree is Apache 2.0. Upstream's 5.8.9 tree also carried 59
directories under `apps/` licensed under the Business Source License 1.1: every
enterprise bridge and the license, enterprise, file transfer and durable storage
applications among them. None of them is here, and nothing from EMQX 5.9 onward
will be, because that code is not Apache. The community build already excluded
those directories by name, so the broker that comes out of this tree is the one
`emqx/emqx:5.8.9` was.

There is no dashboard web UI. Upstream downloaded it at build time as a prebuilt
zip from a repository that carries no license, so it cannot be part of an Apache
distribution. The dashboard application itself is Apache and is untouched: the
REST API on port 18083 is complete, and `openqtt ctl` works. Requests for `/`
answer 404.

## What maintained means here

- Rebuild against a current Erlang/OTP and a current base image.
- Bump dependencies, and take security fixes in them.
- Run upstream's test suite.
- Fix what breaks in the above.

It does not mean feature work, and it does not mean backports from EMQX 5.9 or
later.

## Running it

Images are published to `ghcr.io/openqtt/openqtt` and a Helm chart to
`oci://ghcr.io/openqtt/charts/openqtt`, both from a tagged release. Deploy
the image by the digest recorded in the release notes. The chart's README covers
the two values you must set before installing it.

```
docker run --rm -p 1883:1883 -p 18083:18083 ghcr.io/openqtt/openqtt:1.0.0
```

The version the broker reports is the EMQX version inside, `5.8.9`. OpenQTT's
own version is the image tag and the chart version.

## Upgrading from EMQX

This is a clean break. Nothing reads the old names, and nothing warns you: a
node started with an EMQX environment comes up on its config file's defaults
instead, which is a different cluster with a different cookie. Update the
configuration before you upgrade.

| Was | Is |
| --- | --- |
| `emqx`, `emqx ctl` | `openqtt`, `openqtt ctl` |
| `EMQX_*` environment variables | `OPENQTT_*` |
| `/opt/emqx` in the image | `/opt/openqtt` |
| chart value `emqxConfig` | `openqttConfig` |

- **The binary.** `bin/emqx` is `bin/openqtt`, and `emqx_ctl`,
  `emqx_cluster_rescue` and `emqx_fw` follow the same rule. The release is named
  `openqtt`, so a wrapper that calls the old path finds nothing.
- **The environment.** The hocon override prefix is `OPENQTT_`. Every variable
  moves with it: `OPENQTT_NODE__COOKIE`, `OPENQTT_NODE__NAME`,
  `OPENQTT_CLUSTER__DISCOVERY_STRATEGY`, `OPENQTT_DASHBOARD__DEFAULT_PASSWORD`
  and the rest. There is no `EMQX_` fallback, so a stale variable is not an
  error, it is silence.
- **The container.** The image installs to `/opt/openqtt` and runs as unix user
  `openqtt`. uid and gid stay 1000, so an existing volume's ownership still
  matches, but a mount of `/opt/emqx/data` now mounts an empty directory and the
  node starts with no state.
- **The chart.** `emqxConfig` is `openqttConfig` and every key under it takes
  the `OPENQTT_` prefix. Helm does not reject unknown top level values, so a
  values file that still says `emqxConfig` is ignored rather than refused. The
  data volume is `openqtt-data`, which changes PVC names: persisted state does
  not follow the upgrade, so back it up or plan on losing it.

What did not change: the config keys (`node.name`, `listeners.tcp.default.bind`
and the rest), the REST API and its `/api/v5` paths, `etc/emqx.conf`, the MQTT
wire behaviour, and the Erlang module and application names.

## Building it

`deploy/docker/Dockerfile` builds the community profile from this tree using a
mirror of upstream's builder image under this organisation. `.tool-versions`
names the Erlang and Elixir versions. Pull requests build the image without
pushing it; a `v*` tag builds, publishes, and creates the release.

## Trademarks

EMQX is a trademark of EMQ Technologies Co., Ltd. OpenQTT is not affiliated
with, sponsored by, or endorsed by EMQ. The product, the binary and the
environment variables are named for OpenQTT. The Erlang modules, applications
and config keys are still `emqx`, because renaming them would change nothing
about the software and would make upstream fixes harder to apply.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
