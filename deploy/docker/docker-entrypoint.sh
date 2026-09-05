#!/usr/bin/env bash

## EMQX docker image start script

if [[ -n "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

shopt -s nullglob

## Local IP address setting

LOCAL_IP=$(hostname -i | grep -oE '((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])\.){3}(25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])' | head -n 1)

export OPENQTT_NAME="${OPENQTT_NAME:-emqx}"

## OPENQTT_NODE_NAME or OPENQTT_NODE__NAME to indicate the full node name to be used by EMQX
## If both are set OPENQTT_NODE_NAME takes higher precedence than OPENQTT_NODE__NAME
if [[ -z "${OPENQTT_NODE_NAME:-}" ]] && [[ -z "${OPENQTT_NODE__NAME:-}" ]]; then
    # No node name is provide from environment variables
    # try to resolve from other settings
    if [[ -z "$OPENQTT_HOST" ]]; then
        if [[ "$OPENQTT_CLUSTER__DISCOVERY_STRATEGY" == "dns" ]] && \
            [[ "$OPENQTT_CLUSTER__DNS__RECORD_TYPE" == "srv" ]] && \
            grep -q "$(hostname).$OPENQTT_CLUSTER__DNS__NAME" /etc/hosts; then
                OPENQTT_HOST="$(hostname).$OPENQTT_CLUSTER__DNS__NAME"
        elif [[ "$OPENQTT_CLUSTER__DISCOVERY_STRATEGY" == "k8s" ]] && \
            [[ "$OPENQTT_CLUSTER__K8S__ADDRESS_TYPE" == "dns" ]] && \
            [[ -n "$OPENQTT_CLUSTER__K8S__NAMESPACE" ]]; then
                OPENQTT_CLUSTER__K8S__SUFFIX=${OPENQTT_CLUSTER__K8S__SUFFIX:-"pod.cluster.local"}
                OPENQTT_HOST="${LOCAL_IP//./-}.$OPENQTT_CLUSTER__K8S__NAMESPACE.$OPENQTT_CLUSTER__K8S__SUFFIX"
        elif [[ "$OPENQTT_CLUSTER__DISCOVERY_STRATEGY" == "k8s" ]] && \
            [[ "$OPENQTT_CLUSTER__K8S__ADDRESS_TYPE" == 'hostname' ]] && \
            [[ -n "$OPENQTT_CLUSTER__K8S__NAMESPACE" ]]; then
                OPENQTT_CLUSTER__K8S__SUFFIX=${OPENQTT_CLUSTER__K8S__SUFFIX:-'svc.cluster.local'}
                OPENQTT_HOST=$(grep -h "^$LOCAL_IP" /etc/hosts | grep -o "$(hostname).*.$OPENQTT_CLUSTER__K8S__NAMESPACE.$OPENQTT_CLUSTER__K8S__SUFFIX")
        else
            OPENQTT_HOST="$LOCAL_IP"
        fi
        export OPENQTT_HOST
    fi
    export OPENQTT_NODE_NAME="$OPENQTT_NAME@$OPENQTT_HOST"
fi

# The default rpc port discovery 'stateless' is mostly for clusters
# having static node names. So it's troulbe-free for multiple emqx nodes
# running on the same host.
# When start emqx in docker, it's mostly one emqx node in one container
# i.e. use port 5369 (or per tcp_server_port | ssl_server_port config) for gen_rpc
export OPENQTT_RPC__PORT_DISCOVERY="${OPENQTT_RPC__PORT_DISCOVERY:-manual}"

exec "$@"
