# Codec fuzz targets

Fuzz targets for `openqtt-codec`, run with
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) and libFuzzer. This is a
crate of its own, excluded from the main workspace, because cargo-fuzz needs a
nightly compiler and `make check` runs on the pinned stable one.

| Target | What it checks |
| --- | --- |
| `decode` | Arbitrary bytes, read as one connection's stream, never panic the decoder, and every packet it takes encodes to exactly `encoded_len` bytes. The first byte picks the decoder: with or without a small Maximum Packet Size, told the sender or not. |
| `roundtrip` | Every packet decoded from arbitrary bytes encodes, decodes back to the same packet, and encodes again to the same bytes. |

## Running

```console
rustup toolchain install nightly
cargo install cargo-fuzz
cargo +nightly fuzz run decode
cargo +nightly fuzz run roundtrip
```

Run from the repository root or from `fuzz/`. A run goes on until it finds a
crash or is stopped; give it a time limit with libFuzzer's options after `--`,
here ten minutes on four workers:

```console
cargo +nightly fuzz run decode -- -max_total_time=600 -fork=4
```

`mqtt.dict` holds byte strings libFuzzer would otherwise have to discover, such
as the CONNECT header; pass it with `-dict=fuzz/mqtt.dict` from the repository
root.

The corpus a run builds stays in `fuzz/corpus/<target>/` and a crashing input in
`fuzz/artifacts/<target>/`, both outside version control. Reproduce a crash with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`, then turn it
into a unit test in the codec before fixing it.
