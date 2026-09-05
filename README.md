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
REST API on port 18083 is complete, and `emqx ctl` works. Requests for `/` answer
404.

## What maintained means here

- Rebuild against a current Erlang/OTP and a current base image.
- Bump dependencies, and take security fixes in them.
- Run upstream's test suite.
- Fix what breaks in the above.

It does not mean feature work, and it does not mean backports from EMQX 5.9 or
later.

## Running it

Images are published to `ghcr.io/scadable-iot/openqtt` and a Helm chart to
`oci://ghcr.io/scadable-iot/charts/openqtt`, both from a tagged release. Deploy
the image by the digest recorded in the release notes. The chart's README covers
the two values you must set before installing it.

```
docker run --rm -p 1883:1883 -p 18083:18083 ghcr.io/scadable-iot/openqtt:1.0.0
```

The version the broker reports is the EMQX version inside, `5.8.9`. OpenQTT's
own version is the image tag and the chart version.

## Building it

`deploy/docker/Dockerfile` builds the community profile from this tree using a
mirror of upstream's builder image under this organisation. `.tool-versions`
names the Erlang and Elixir versions. Pull requests build the image without
pushing it; a `v*` tag builds, publishes, and creates the release.

## Trademarks

EMQX is a trademark of EMQ Technologies Co., Ltd. OpenQTT is not affiliated
with, sponsored by, or endorsed by EMQ. The name of the binary, the modules and
the environment variables remain `emqx` because renaming them would change
nothing about the software and would make upstream fixes harder to apply.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
