#!/usr/bin/env bash

set -euo pipefail
set -x

MAKE_TARGET="${1:-}"

case "${MAKE_TARGET}" in
    emqx-enterprise-*)
        OPENQTT_NAME='emqx-enterprise'
        ;;
    emqx-*)
        OPENQTT_NAME='emqx'
        ;;
    *)
        echo "Usage $0 <PKG_TARGET>"
        exit 1
        ;;
esac

case "${MAKE_TARGET}" in
    *-tgz)
        PACKAGE_TYPE='tgz'
        ;;
    *-pkg)
        PACKAGE_TYPE='pkg'
        ;;
    *)
        echo "Unknown package type ${1}"
        exit 2
        ;;
esac

case "${MAKE_TARGET}" in
    *elixir*)
        IS_ELIXIR='yes'
        ;;
    *)
        IS_ELIXIR='no'
        ;;
esac

export DEBUG=1
export CODE_PATH=${CODE_PATH:-"/emqx"}
export SCRIPTS="${CODE_PATH}/scripts"
export OPENQTT_NAME
export PACKAGE_PATH="${CODE_PATH}/_packages/${OPENQTT_NAME}"
export RELUP_PACKAGE_PATH="${CODE_PATH}/_upgrade_base"
export PAHO_MQTT_TESTING_PATH="${PAHO_MQTT_TESTING_PATH:-/paho-mqtt-testing}"

SYSTEM="$("$SCRIPTS"/get-distro.sh)"

if [ "$PACKAGE_TYPE" = 'tgz' ]; then
    PKG_SUFFIX="tar.gz"
else
    case "${SYSTEM:-}" in
        ubuntu*|debian*|raspbian*)
            PKG_SUFFIX='deb'
            ;;
        *)
            PKG_SUFFIX='rpm'
            ;;
    esac
fi
PACKAGE_VERSION="$("$CODE_PATH"/pkg-vsn.sh "${OPENQTT_NAME}")"
PACKAGE_VERSION_LONG="$("$CODE_PATH"/pkg-vsn.sh "${OPENQTT_NAME}" --long --elixir "${IS_ELIXIR}")"
PACKAGE_NAME="${OPENQTT_NAME}-${PACKAGE_VERSION_LONG}"
PACKAGE_FILE_NAME="${PACKAGE_FILE_NAME:-${PACKAGE_NAME}.${PKG_SUFFIX}}"

PACKAGE_FILE="${PACKAGE_PATH}/${PACKAGE_FILE_NAME}"
if ! [ -f "$PACKAGE_FILE" ]; then
    echo "$PACKAGE_FILE is not a file"
    exit 1
fi

emqx_prepare(){
    mkdir -p "${PACKAGE_PATH}"

    if [ ! -d "${PAHO_MQTT_TESTING_PATH}" ]; then
        git clone -b develop-4.0 https://github.com/emqx/paho.mqtt.testing.git "${PAHO_MQTT_TESTING_PATH}"
    fi
    # Debian 12/13 and Ubuntu 24.04 complain if we don't use venv
    case "${SYSTEM:-}" in
        debian12|debian13|ubuntu24.04)
            apt-get update -y && apt-get install -y virtualenv
            virtualenv venv
            # https://www.shellcheck.net/wiki/SC1091
            # shellcheck source=/dev/null
            source ./venv/bin/activate
            ;;
        *)
            ;;
    esac
    pip3 install -r "$CODE_PATH/scripts/pytest.requirements.txt"
}

emqx_test(){
    cd "${PACKAGE_PATH}"
    local packagename="${PACKAGE_FILE_NAME}"
    case "$PKG_SUFFIX" in
        "tar.gz")
            mkdir -p "${PACKAGE_PATH}/emqx"
            tar -C "${PACKAGE_PATH}/emqx" -zxf "${PACKAGE_PATH}/${packagename}"
            export OPENQTT_ZONES__DEFAULT__MQTT__SERVER_KEEPALIVE=60
            export OPENQTT_MQTT__MAX_TOPIC_ALIAS=10
            export OPENQTT_LOG__CONSOLE_HANDLER__LEVEL=debug
            export OPENQTT_LOG__FILE_HANDLERS__DEFAULT__LEVEL=debug
            # if [[ $(arch) == *arm* || $(arch) == aarch64 ]]; then
            #     export OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLED=false
            # fi
            # sed -i '/emqx_telemetry/d' "${PACKAGE_PATH}"/emqx/data/loaded_plugins

            run_test "${PACKAGE_PATH}/emqx/bin" "${PACKAGE_PATH}/emqx/log" "${PACKAGE_PATH}/emqx/releases/openqtt_vars"

            rm -rf "${PACKAGE_PATH}"/emqx
        ;;
        "deb")
            dpkg -i "${PACKAGE_PATH}/${packagename}"
            if [ "$(dpkg -l | grep ${OPENQTT_NAME} | awk '{print $1}')" != "ii" ]
            then
                echo "package install error"
                exit 1
            fi

            run_test "/usr/bin" "/var/log/emqx" "$(dpkg -L ${OPENQTT_NAME} | grep openqtt_vars)"

            dpkg -r "${OPENQTT_NAME}"
            if [ "$(dpkg -l | grep ${OPENQTT_NAME} | awk '{print $1}')" != "rc" ]
            then
                echo "package remove error"
                exit 1
            fi

            echo "try to install again and purge while the service is running"
            dpkg -i "${PACKAGE_PATH}/${packagename}"
            if [ "$(dpkg -l | grep ${OPENQTT_NAME} | awk '{print $1}')" != "ii" ]
            then
                echo "package install error"
                exit 1
            fi
            if ! /usr/bin/openqtt start
            then
                echo "ERROR: failed_to_start_emqx"
                cat /var/log/emqx/erlang.log.1 || true
                cat /var/log/emqx/emqx.log.1 || true
                exit 1
            fi
            /usr/bin/openqtt ping
            dpkg -P "${OPENQTT_NAME}"
            if dpkg -l |grep -q emqx
            then
                echo "package uninstall error"
                exit 1
            fi
        ;;
        "rpm")
            # yum wants python2
            case "${SYSTEM:-}" in
                "el8")
                    # el8 is fine with python3
                    true
                    ;;
                "el9")
                    # el9 is fine with python3
                    true
                    ;;
                *)
                    alternatives --list | grep python && alternatives --set python /usr/bin/python2
                    ;;
            esac
            YUM_RES=$(yum install -y "${PACKAGE_PATH}/${packagename}"| tee /dev/null)
            if [[ $YUM_RES =~ "Failed" ]]; then
               echo "yum install failed"
               exit 1
            fi
            alternatives --list | grep python && alternatives --set python /usr/bin/python3
            if ! rpm -q "${OPENQTT_NAME}" | grep -q "${OPENQTT_NAME}"; then
                echo "package install error"
                exit 1
            fi

            run_test "/usr/bin" "/var/log/emqx" "$(rpm -ql ${OPENQTT_NAME} | grep openqtt_vars)"

            rpm -e "${OPENQTT_NAME}"
            if [ "$(rpm -q ${OPENQTT_NAME})" != "package ${OPENQTT_NAME} is not installed" ];then
                echo "package uninstall error"
                exit 1
            fi
        ;;
    esac
}

run_test(){
    local bin_dir="$1"
    local log_dir="$2"
    local openqtt_env_vars="$3"
    # sed -i '/emqx_telemetry/d' /var/lib/emqx/loaded_plugins

    if [ -f "$openqtt_env_vars" ];
    then
        tee -a "$openqtt_env_vars" <<EOF
export OPENQTT_ZONES__DEFAULT__MQTT__SERVER_KEEPALIVE=60
export OPENQTT_MQTT__MAX_TOPIC_ALIAS=10
export OPENQTT_LOG__CONSOLE_HANDLER__LEVEL=debug
export OPENQTT_LOG__FILE_HANDLERS__DEFAULT__LEVEL=debug
EOF
        ## for ARM, due to CI env issue, skip start of quic listener for the moment
        # [[ $(arch) == *arm* || $(arch) == aarch64 ]] && tee -a "$openqtt_env_vars" <<EOF
# export OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLED=false
# EOF
    else
        echo "Error: cannot locate openqtt_vars"
        exit 1
    fi
    if ! "${bin_dir}/openqtt" 'start' 'help'; then
        echo "ERROR: failed_to_call_help_command"
        exit 1
    fi
    if ! "${bin_dir}/openqtt" 'help'; then
        echo "ERROR: failed_to_call_help_command"
        exit 1
    fi
    echo "running ${packagename} start"
    if ! "${bin_dir}/openqtt" 'start'; then
        echo "ERROR: failed_to_start_emqx"
        cat "${log_dir}/erlang.log.1" || true
        cat "${log_dir}/emqx.log.1" || true
        exit 1
    fi
    "$SCRIPTS/test/emqx-smoke-test.sh" 127.0.0.1 18083
    pytest -v "${PAHO_MQTT_TESTING_PATH}"/interoperability/test_client/V5/test_connect.py::test_basic
    "${bin_dir}/openqtt" ping
    echo "running ${packagename} stop"
    if ! "${bin_dir}/openqtt" 'stop'; then
        echo "ERROR: failed_to_stop_emqx_with_the_stop_command"
        cat "${log_dir}/erlang.log.1" || true
        cat "${log_dir}/emqx.log.1" || true
        exit 1
    fi
}

relup_test(){
    if [ ! -d "${RELUP_PACKAGE_PATH}" ]; then
        echo "WARNING: ${RELUP_PACKAGE_PATH} is not a dir, skipped relup test!"
        return 0
    fi
    cd "${RELUP_PACKAGE_PATH}"
    local pattern
    pattern="$OPENQTT_NAME-$("$CODE_PATH"/pkg-vsn.sh "${OPENQTT_NAME}" --long --vsn_matcher)"
    while read -r pkg; do
        packagename=$(basename "${pkg}")
        mkdir -p emqx
        tar -C emqx -zxf "$packagename"
        if ! ./emqx/bin/openqtt start; then
            cat emqx/log/erlang.log.1 || true
            cat emqx/log/emqx.log.1 || true
            exit 1
        fi
        ./emqx/bin/openqtt_ctl status
        ./emqx/bin/openqtt versions
        cp "${PACKAGE_PATH}/${PACKAGE_NAME}.tar.gz" ./emqx/releases/
        ./emqx/bin/openqtt install "${PACKAGE_VERSION}"
        [ "$(./emqx/bin/openqtt versions | grep permanent | awk '{print $2}')" = "${PACKAGE_VERSION}" ] || exit 1
        ./emqx/bin/openqtt_ctl status
        ./emqx/bin/openqtt stop
        rm -rf emqx
    done < <(find . -maxdepth 1 -name "${pattern}.tar.gz")
}

emqx_prepare
emqx_test
if [ "$IS_ELIXIR" = 'yes' ]; then
    echo "WARNING: skipped relup test for elixir"
else
    relup_test
fi
