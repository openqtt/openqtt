# THE BUILDER IS A MIRROR OF UPSTREAM'S, copied by digest into this organisation
# so that a build never depends on EMQ keeping an image published. The release
# workflow refreshes the mirror before it builds.
ARG BUILD_FROM=ghcr.io/openqtt/emqx-builder:5.6-2-1.15.7-26.2.5.14-1-debian13
ARG RUN_FROM=debian:13-slim
ARG SOURCE_TYPE=src # tgz

FROM ${BUILD_FROM} AS builder_src
ONBUILD COPY . /emqx

FROM ${RUN_FROM} AS builder_tgz
ARG PROFILE=emqx
ARG PKG_VSN
ARG SUFFIX
ARG TARGETARCH
ONBUILD COPY ${PROFILE}-${PKG_VSN}${SUFFIX}-debian13-$TARGETARCH.tar.gz /${PROFILE}.tar.gz

FROM builder_${SOURCE_TYPE} AS builder

ARG PROFILE=emqx
ARG IS_ELIXIR=no
ARG DEBUG

ENV EMQX_RELUP=false
ENV EMQX_REL_FORM='docker'
# English descriptions are generated from this tree. The Chinese translation is
# a build-time download from a moving branch of emqx/emqx-i18n, which makes two
# builds of one commit differ; a release build fetches nothing it does not pin.
ENV DOWNLOAD_I18N_TRANSLATIONS=false

WORKDIR /emqx/

RUN mkdir -p /emqx-rel/emqx && \
    if [ -f "/${PROFILE}.tar.gz" ]; then \
      tar zxf "/${PROFILE}.tar.gz" -C /emqx-rel/emqx; \
    else \
      if [ -f rebar-git-cache.tar ]; then \
        mkdir .cache && \
        tar -xf rebar-git-cache.tar -C .cache && \
        export REBAR_GIT_CACHE_DIR='/emqx/.cache' && \
        export REBAR_GIT_CACHE_REF_AUTOFILL=0; \
      fi && \
      export EMQX_REL_PATH="/emqx/_build/${PROFILE}/rel/emqx" && \
      git config --global --add safe.directory '*' && \
      make ${PROFILE}-tgz && \
      tar zxf _packages/${PROFILE}/*.tar.gz -C /emqx-rel/emqx; \
    fi

FROM $RUN_FROM
ARG EXTRA_DEPS=''

# Elixir complains if runs without UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY deploy/docker/docker-entrypoint.sh /usr/bin/

RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates procps curl $(echo "${EXTRA_DEPS}" | tr ',' ' '); \
    rm -rf /var/lib/apt/lists/*; \
    groupadd -r -g 1000 emqx; \
    useradd -r -m -u 1000 -g emqx emqx;

COPY --from=builder --chown=emqx:emqx /emqx-rel /opt/

RUN ln -s /opt/emqx/bin/* /usr/local/bin/

WORKDIR /opt/emqx

USER emqx

VOLUME ["/opt/emqx/log", "/opt/emqx/data"]

# emqx will occupy these port:
# - 1883 port for MQTT
# - 8083 for WebSocket/HTTP
# - 8084 for WSS/HTTPS
# - 8883 port for MQTT(SSL)
# - 18083 for dashboard and API
# - 4370 default Erlang distribution port
# - 5369 for backplane gen_rpc
EXPOSE 1883 8083 8084 8883 18083 4370 5369

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["/opt/emqx/bin/emqx", "foreground"]
