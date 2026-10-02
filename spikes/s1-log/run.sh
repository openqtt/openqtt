#!/bin/sh
# Runs spike S1's experiments. Each step appends JSON lines to out/<step>.jsonl; collect.py
# folds them into bench/results. Long steps take tens of minutes: run them one at a time on an
# otherwise quiet machine, and repeat a step to see its spread.
#
#   ./run.sh <step> [bench binary]
#
# DATA is where databases go (tens of GiB for recovery); it is wiped per run.
set -eu
cd "$(dirname "$0")"
STEP=$1
BIN=${2:-target/release/s1-bench}
DATA=${DATA:-/tmp/s1-data}
OUT=out
mkdir -p "$OUT" "$DATA"
b() { "$BIN" --data "$DATA" --out "$OUT/$STEP.jsonl" "$@"; }

case "$STEP" in
fsync)
    b fsync --threads 1,3 --secs 5
    ;;
write)
    # Three passes; each pass interleaves the engines inside every configuration.
    for pass in 1 2 3; do
        b write --warmup 2 --secs 8
    done
    ;;
shared)
    for pass in 1 2 3; do
        b shared --warmup 2 --secs 8
    done
    ;;
*)
    echo "unknown step: $STEP" >&2
    exit 1
    ;;
esac
