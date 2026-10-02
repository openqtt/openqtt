Telemetry is now off unless `telemetry.enable` is set to `true`.

With the setting unset, `emqx_telemetry` fell back to whether the version string
looks like an official EMQX release, so a build stamped `5.8.9` posted a usage
report to `https://telemetry.emqx.io/api/telemetry` 10 seconds after it started
and every 7 days after that. The published 1.0.0 images are stamped
`5.8.9-g49aaa3f5`, which does not match, so they sent nothing. The fallback is
now `false`, the schema default for `telemetry.enable` is `false`, and the REST
API status body defaults to `false`. Turn reporting on with
`OPENQTT_TELEMETRY__ENABLE=true` or `PUT /api/v5/telemetry/status`.
