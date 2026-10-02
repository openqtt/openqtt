# OpenQTT 2.0: the openqtt binary, built with the pinned compiler and copied into a distroless
# runtime that has no shell and no package manager. Each architecture builds natively.
#
# RUST_VERSION matches rust-toolchain.toml, which still decides: rustup in the build stage reads
# it, so a stale RUST_VERSION costs a download, never a different compiler. The build stage is
# Debian 12 because the runtime is: a binary linked on a newer Debian can need glibc symbols
# that cc-debian12 does not have.
ARG RUST_VERSION=1.99.0

FROM rust:${RUST_VERSION}-bookworm AS build
WORKDIR /src
COPY . .
# The cache mounts keep the registry and target/ between local builds; the binary is copied out
# of target/ because a cache mount is not part of the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p openqtt \
    && cp target/release/openqtt /openqtt

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /openqtt /openqtt
# 14567/udp: MQTT 5 over QUIC, the client listener. 8080/tcp: HTTP for the health probes, and
# the admin API where the admin role runs.
EXPOSE 14567/udp 8080/tcp
ENTRYPOINT ["/openqtt"]
CMD ["run"]
