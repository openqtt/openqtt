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

# Starts a durable writer, lets it write for $2 seconds, then kills it with SIGKILL.
crash() {
    engine=$1
    secs=$2
    shift 2
    log="$OUT/crash-$engine.log"
    rm -f "$log"
    "$BIN" --data "$DATA" --out "$OUT/$STEP.jsonl" recovery --engine "$engine" --phase crash-writer "$@" >"$log" 2>&1 &
    pid=$!
    while ! grep -q writing "$log" 2>/dev/null; do sleep 0.2; done
    sleep "$secs"
    kill -9 "$pid"
    wait "$pid" 2>/dev/null || true
}

# One JSON line for a build from an empty target directory: feature, profile, wall seconds,
# binary size.
build_one() {
    feature=$1
    profile=$2
    dir=target/build-$feature-$profile
    rm -rf "$dir"
    start=$(python3 -c 'import time; print(time.time())')
    cargo build --locked --quiet --profile "$profile" -p s1-probe --features "$feature" --target-dir "$dir"
    secs=$(python3 -c "import time; print(round(time.time() - $start, 1))")
    sub=$profile
    [ "$profile" = dev ] && sub=debug
    size=$(stat -f %z "$dir/$sub/s1-probe" 2>/dev/null || stat -c %s "$dir/$sub/s1-probe")
    echo "{\"exp\":\"build\",\"where\":\"$(uname -s)-$(uname -m)\",\"feature\":\"$feature\",\"profile\":\"$profile\",\"secs\":$secs,\"binary_bytes\":$size}" | tee -a "$OUT/$STEP.jsonl"
}

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
claims)
    b claims --sessions 1000000 --workers 4
    # One apply thread: the most a single partition's state machine sustains.
    b claims --sessions 1000000 --workers 1 --rates 5000,10000,25000,50000,100000
    ;;
footprint)
    for n in 1000000 10000000; do
        for e in fjall rocksdb redb; do
            b footprint --engine "$e" --sessions "$n" --phase load --cache-mb 1024
            b footprint --engine "$e" --sessions "$n" --phase idle --cache-mb 64
            b footprint --engine "$e" --sessions "$n" --phase remove
        done
    done
    ;;
churn)
    for e in fjall rocksdb redb; do
        b churn --engine "$e" --secs 600
    done
    ;;
recovery)
    GIB=${GIB:-10}
    for e in fjall rocksdb redb; do
        b recovery --engine "$e" --phase load --gib "$GIB" --sync-every-mb 1
        b recovery --engine "$e" --phase open --label clean
        crash "$e" 10
        b recovery --engine "$e" --phase open --label crash
        b recovery --engine "$e" --phase replay --entries 1000000
        b recovery --engine "$e" --phase remove
    done
    # redb's quick repair: allocator state saved on every commit, so a crash needs no full walk.
    # Its recovery does not depend on size, so a smaller database shows it.
    b recovery --engine redb --phase load --gib 2 --sync-every-mb 1 --quick-repair
    crash redb 10 --quick-repair
    b recovery --engine redb --phase open --label crash --quick-repair
    b recovery --engine redb --phase remove
    ;;
window)
    # Windows interleaved with each other, not minutes apart, so drift in the machine's fsync
    # cost does not pass for an effect of the window.
    for pass in 1 2 3; do
        for load in c1 c64 o10000 o50000; do
            for w in 0 1000 2000; do
                b write --sizes 128 --windows-us "$w" --loads "$load" --warmup 2 --secs 8
            done
        done
    done
    ;;
repl)
    b repl --schemes raft,raft-batched,raft10,pb --groups 1,16,128 --delays-us 1000,2000 --flush-us 0,1000 \
        --loads o1000,o20000
    ;;
repl-storm)
    # The reconnect-storm target, 50,000 claims a second across the cluster.
    b repl --schemes raft10,pb,raft-batched,raft --groups 16,128,256 --delays-us 1000 --flush-us 1000 \
        --loads o50000
    ;;
repl-max)
    # The most one partition leader commits: one group, writes always outstanding.
    b repl --schemes raft,raft-batched,raft10,pb --groups 1 --delays-us 1000 --flush-us 0,1000 \
        --loads c16,c256,c1024
    ;;
idle)
    b idle --schemes raft,raft10,pb --groups 128,256 --timings 50:150:300,250:1000:2000 --secs 20
    ;;
netcost)
    b netcost --secs 5 --size 128
    ;;
build)
    for f in none fjall redb rocksdb openraft; do
        build_one "$f" probe
        build_one "$f" dev
    done
    ;;
*)
    echo "unknown step: $STEP" >&2
    exit 1
    ;;
esac
