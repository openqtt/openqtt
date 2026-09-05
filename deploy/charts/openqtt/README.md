# OpenQTT Helm chart

Deploys OpenQTT, the Apache 2.0 continuation of EMQX 5.8, as a StatefulSet
that clusters itself through DNS discovery on its headless Service.

The chart started as upstream's `emqx` chart at 5.8.9. Values are documented in
`values.yaml`. Everything operator-facing now reads `openqtt`: the values key is
`openqttConfig`, broker variables carry the `OPENQTT_` prefix, and the template
helpers are `openqtt.name`, `openqtt.fullname` and friends.

## Installing

Each release attaches the packaged chart to the GitHub release:

    helm install openqtt https://github.com/openqtt/OpenQTT/releases/download/v0.1.0/openqtt-0.1.0.tgz

or from the OCI registry:

    helm install openqtt oci://ghcr.io/openqtt/charts/openqtt --version 0.1.0

## The one value you must set

`nodeCookie` is the Erlang distribution secret that every node in the cluster
shares. It has no default and the chart refuses to render without it, because the
image falls back to `emqxsecretcookie`, a string published in EMQX's git history.
Anything that can reach port 4370 knowing it gets an Erlang distribution
connection, which is code execution inside the broker.

    helm install openqtt oci://ghcr.io/openqtt/charts/openqtt --version 0.1.0 \
      --set nodeCookie=$(openssl rand -hex 32)

`dashboardPassword` sets the initial password for the REST API `admin` user. Left
empty it stays the upstream default, `public`, which is fine only for a throwaway
cluster.

Both are written to a chart-managed Secret and injected after the ConfigMap, so
they never appear in the ConfigMap and they win at runtime. Do not put either key
in `openqttConfig`: everything there is serialised verbatim into a world-readable
ConfigMap, and the chart fails the render if it sees them.

If you manage secrets yourself, point `envFromSecret` at your own Secret carrying
`OPENQTT_NODE__COOKIE`; that satisfies the guard and is applied last.

## Pinning the image

`image.digest` pins by digest and wins over `image.tag`. The tag defaults to the
chart version, not to `appVersion`: `appVersion` is the EMQX version inside the
image (5.8.9) and is never published as a tag.

    --set image.digest=sha256:...

## What is not here

No dashboard web UI. The REST API on port 18083 is complete; the static files
behind `/` are absent because their upstream source carries no license.
