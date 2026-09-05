#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../.."
# shellcheck disable=SC1091
source ./env.sh

PROFILE="${PROFILE:-emqx}"
OPENQTT_ROOT="${OPENQTT_ROOT:-_build/$PROFILE/rel/openqtt}"
OPENQTT_WAIT_FOR_START="${OPENQTT_WAIT_FOR_START:-30}"
export OPENQTT_WAIT_FOR_START

function check_dashboard_https_ssl_options_depth() {
  if [[ $1 =~ v5\.0\.25 ]]; then
    EXPECT_DEPTH=5
  else
    EXPECT_DEPTH=10
  fi
  DEPTH=$("$OPENQTT_ROOT"/bin/openqtt eval "emqx:get_config([dashboard,listeners,https,ssl_options,depth],10)")
  if [[ "$DEPTH" != "$EXPECT_DEPTH" ]]; then
    echo "Bad Https depth $DEPTH, expect $EXPECT_DEPTH"
    exit 1
  fi
}

start_emqx_with_conf() {
    echo "Starting $PROFILE with $1"
    "$OPENQTT_ROOT"/bin/openqtt start
    check_dashboard_https_ssl_options_depth "$1"
    "$OPENQTT_ROOT"/bin/openqtt stop
}

PKG_VSN=${PKG_VSN:-$(./pkg-vsn.sh "$PROFILE")}
MAJOR_VSN=$(echo "$PKG_VSN" | cut -d- -f1 | cut -d. -f1)

if [ "$PROFILE" = "emqx" ]; then
  PREFIX="v"
else
  PREFIX="e"
fi

FILES=$(ls ./scripts/conf-test/old-confs/"${PREFIX}${MAJOR_VSN}"*)

cp "$OPENQTT_ROOT"/etc/emqx.conf "$OPENQTT_ROOT"/etc/emqx.conf.bak
cleanup() {
    cp "$OPENQTT_ROOT"/etc/emqx.conf.bak "$OPENQTT_ROOT"/etc/emqx.conf
}
trap cleanup EXIT

for file in $FILES; do
    cp "$file" "$OPENQTT_ROOT"/etc/emqx.conf
    start_emqx_with_conf "$file"
done
