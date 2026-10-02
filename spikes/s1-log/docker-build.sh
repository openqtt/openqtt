#!/bin/sh
# Build friction where the broker is built and run: each engine compiled from an empty target
# directory in rust:1.99.0-bookworm (the Dockerfile's build stage, native architecture), then the
# binary started in gcr.io/distroless/cc-debian12:nonroot (its runtime). Crates are fetched once
# beforehand, so the times are compile times, not downloads.
#
#   ./docker-build.sh
#
# Appends JSON lines to out/build.jsonl.
set -eu
cd "$(dirname "$0")"
OUT=out/build.jsonl
BUILD=rust:1.99.0-bookworm
RUN=gcr.io/distroless/cc-debian12:nonroot
mkdir -p out
docker pull -q "$BUILD" >/dev/null
docker pull -q "$RUN" >/dev/null
# Crates go to a named volume shared by every run below.
docker volume create s1-cargo >/dev/null
docker run --rm -v "$PWD":/src:ro -v s1-cargo:/usr/local/cargo/registry -w /src "$BUILD" \
    cargo fetch --locked >/dev/null 2>&1

now() { python3 -c 'import time; print(time.time())'; }

# $1 feature, $2 shell prelude run before the build (installing packages), $3 label.
build() {
    feature=$1
    prelude=$2
    label=$3
    start=$(now)
    set +e
    log=$(docker run --rm -v "$PWD":/src:ro -v s1-cargo:/usr/local/cargo/registry -w /src "$BUILD" sh -c "
        $prelude
        start=\$(date +%s%N)
        cargo build --offline --locked --quiet --profile probe -p s1-probe --features $feature --target-dir /tmp/t || exit 3
        end=\$(date +%s%N)
        echo BUILD_MS=\$(( (end - start) / 1000000 ))
        echo BINARY_BYTES=\$(stat -c %s /tmp/t/probe/s1-probe)
    " 2>&1)
    status=$?
    set -e
    total=$(python3 -c "import time; print(round(time.time() - $start, 1))")
    build_ms=$(echo "$log" | sed -n 's/^BUILD_MS=//p' | tail -1)
    secs=$([ -n "$build_ms" ] && python3 -c "print(round($build_ms / 1000, 1))" || true)
    size=$(echo "$log" | sed -n 's/^BINARY_BYTES=//p' | tail -1)
    err=$(echo "$log" | grep -E 'error|Unable to find libclang|could not find' | head -3 | tr '"' "'" | tr '\n' ' ' | cut -c1-400)
    echo "{\"exp\":\"build\",\"where\":\"docker-$BUILD-$(uname -m)\",\"feature\":\"$feature\",\"profile\":\"probe\",\"variant\":\"$label\",\"ok\":$([ $status -eq 0 ] && echo true || echo false),\"secs\":${secs:-null},\"container_secs\":$total,\"binary_bytes\":${size:-null},\"error\":\"$err\"}" | tee -a "$OUT"
}

for f in none fjall redb openraft; do
    build "$f" "" "plain image"
done
# RocksDB's bindgen needs libclang, which the rust image does not have.
build rocksdb "" "plain image"
build rocksdb "apt-get update -qq >/dev/null && apt-get install -y -qq libclang-dev >/dev/null" "with libclang-dev"

# Does a binary linking RocksDB (libstdc++) start in the distroless runtime?
docker run --rm -v "$PWD":/src:ro -v s1-cargo:/usr/local/cargo/registry -v s1-probe-bin:/bin-out -w /src "$BUILD" sh -c "
    apt-get update -qq >/dev/null && apt-get install -y -qq libclang-dev >/dev/null
    cargo build --offline --locked --quiet --profile probe -p s1-probe --features rocksdb --target-dir /tmp/t
    cp /tmp/t/probe/s1-probe /bin-out/s1-probe-rocksdb" >/dev/null 2>&1
set +e
ran=$(docker run --rm -v s1-probe-bin:/b "$RUN" /b/s1-probe-rocksdb 2>&1)
st=$?
set -e
# distroless has no default command, so create needs one; it is never run.
cid=$(docker create "$RUN" /nonexistent)
libs=$(docker export "$cid" | tar -t 2>/dev/null | grep -E 'libstdc\+\+\.so\.6\.|libgcc_s' | tr '\n' ' ')
docker rm "$cid" >/dev/null
echo "{\"exp\":\"runtime\",\"image\":\"$RUN\",\"binary\":\"probe with rocksdb\",\"exit\":$st,\"output\":\"$(echo "$ran" | tr '"' "'" | tr '\n' ' ' | cut -c1-200)\",\"cxx_libs_in_image\":\"$libs\"}" | tee -a "$OUT"
