# OpenQTT 2.0 image

**For production, use the 1.0.x images** (`ghcr.io/openqtt/openqtt:1.0.N`), built
from branch `release/1.x`. This image is 2.0, a pre-release: `openqtt run` is
not implemented yet.

`Dockerfile` builds the `openqtt` binary with the compiler that
`rust-toolchain.toml` pins and copies it into
`gcr.io/distroless/cc-debian12:nonroot`: no shell, no package manager, and a
non-root user (uid 65532).

```console
docker build -f deploy/docker/Dockerfile -t openqtt:dev .
docker run --rm openqtt:dev --help
```

The entrypoint is `/openqtt` and the default command is `run`.

| Port | Use |
| --- | --- |
| 14567/udp | MQTT 5 over QUIC, the client listener |
| 8080/tcp | HTTP: the health probes, and the admin API where the admin role runs |

Pull requests build the image for `linux/amd64` without pushing it. Nothing is
published from `main` until the 2.0 release workflow exists.
