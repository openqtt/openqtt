#!/bin/sh
# Runs every S2 measurement and writes the JSON that report R6 quotes into bench/results/.
# Run from anywhere; takes about fifteen minutes and up to 3 GiB of memory on a laptop.
# Pass experiment names to run only those: ./run.sh match fanout
set -eu
here=$(cd "$(dirname "$0")" && pwd)
cd "$here"
# Relative paths, so that the arguments recorded in the results do not name this checkout.
out=../../bench/results
mkdir -p "$out"
cargo build --release --locked
bin="$here/target/release/openqtt-spike-s2"
run() {
    name=$1
    shift
    if [ -n "${ONLY:-}" ] && ! echo " $ONLY " | grep -q " $name "; then return 0; fi
    echo "== $name"
    "$bin" "$@"
}
ONLY="$*"
run memory memory --workload a --n 100000,1000000,10000000 --out "$out/s2-memory-devices.json"
run memory-mixed memory --workload b --n 1000000 --out "$out/s2-memory-mixed.json"
run memory-shared memory --workload c --n 1000000 --out "$out/s2-memory-shared.json"
run match match --n 1000000 --samples 1000000 --out "$out/s2-match.json"
run match-1e5 match --n 100000 --samples 1000000 --out "$out/s2-match-1e5.json"
run fanout fanout --background 1000000 --out "$out/s2-fanout.json"
run coarsen-1e5 coarsen --n 100000 --samples 200000 --t 16,64,256 --floor 1,3 --full-view 0 \
    --out "$out/s2-coarsen-1e5.json"
run coarsen coarsen --n 1000000 --samples 200000 --out "$out/s2-coarsen-1e6.json"
run coarsen-1e7 coarsen --n 10000000 --samples 200000 --full-view 0 --t 16,64,256,1024 --floor 1,3 \
    --out "$out/s2-coarsen-1e7.json"
run churn churn --n 1000000 --minutes 6 --out "$out/s2-churn.json"
