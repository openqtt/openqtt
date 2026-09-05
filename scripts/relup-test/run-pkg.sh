#!/usr/bin/env bash

## This script is intended to run in docker
## extracts a .tar.gz package and runs EMQX in console mode

set -euo pipefail

PKG="$1"

mkdir -p emqx
tar -C emqx -zxf "$PKG"

ln -s "$(pwd)/emqx/bin/openqtt" /usr/bin/openqtt
ln -s "$(pwd)/emqx/bin/openqtt_ctl" /usr/bin/openqtt_ctl

if command -v apt; then
    apt update -y
    apt install -y \
        curl \
        jq \
        libffi-dev \
        libkrb5-3 \
        libkrb5-dev \
        libncurses5-dev \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules-gssapi-mit \
        libssl-dev \
        zip
fi

openqtt console
