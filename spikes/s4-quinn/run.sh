#!/bin/sh
# Runs every S4 measurement and writes the JSON that report R7 quotes into bench/results/.
# Server and client run as two processes on this machine; nothing else should be busy.
# Pass experiment names to run only those: ./run.sh handshake
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out="$here/../../bench/results"
mkdir -p "$out"
cd "$here"
cargo build --release --locked
bin="$here/target/release/s4"
pki="$here/target/pki"
[ -f "$pki/clients.key" ] || "$bin" pki --pki "$pki"
run() {
    name=$1
    shift
    if [ -n "${ONLY:-}" ] && ! echo " $ONLY " | grep -q " $name "; then return 0; fi
    echo "== $name"
    "$bin" "$@" --pki "$pki"
}
ONLY="$*"
run single idle --conns 1000,10000,50000 --endpoints 1 --profiles default \
    --out "$out/s4-idle-one-endpoint.json"
run per-core idle --conns 10000,50000 --endpoints 10 --profiles default \
    --out "$out/s4-idle-endpoint-per-core.json"
run stream-keepalive idle --conns 10000 --keepalive stream --profiles default \
    --out "$out/s4-idle-stream-keepalive.json"
run profiles idle --conns 10000 --window 30 \
    --profiles streams,windows,no-datagrams,no-mtu-discovery,ack-frequency,lean \
    --out "$out/s4-idle-transport-profiles.json"
run lean idle --conns 50000 --profiles lean --out "$out/s4-idle-lean-50k.json"
run handshake handshake --lanes 128 --duration 20 --out "$out/s4-handshake.json"
