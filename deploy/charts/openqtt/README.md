# OpenQTT Helm chart

Deploys OpenQTT, the Apache 2.0 continuation of EMQX 5.8, as a StatefulSet
that clusters itself through DNS discovery on its headless Service.

The chart is upstream's `emqx` chart at 5.8.9 with the name and the default
image changed. Values are documented in `values.yaml`. Template helpers keep
their `emqx.*` names so that upstream fixes still apply cleanly.

## Installing

Each release attaches the packaged chart to the GitHub release:

    helm install openqtt https://github.com/SCADABLE-IOT/OpenQTT/releases/download/v1.0.0/openqtt-1.0.0.tgz

or from the OCI registry:

    helm install openqtt oci://ghcr.io/scadable-iot/charts/openqtt --version 1.0.0

## Two values you must set

`envFromSecret` names a Secret that carries `EMQX_NODE__COOKIE`, the Erlang
distribution secret every node must share, and `EMQX_DASHBOARD__DEFAULT_PASSWORD`.
The chart's defaults for both are public strings. Create the Secret before
installing and set `emqxConfig.EMQX_DASHBOARD__DEFAULT_PASSWORD` to an empty
string so the default is not also written to the ConfigMap.

## What is not here

No dashboard web UI. The REST API on port 18083 is complete; the static files
behind `/` are absent because their upstream source carries no license.
