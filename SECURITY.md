# Security

## Supported versions

| Version | Supported |
| --- | --- |
| 1.0.x | Security fixes, until six months after 2.0.0 is released |
| 2.0 pre-releases and `main` | No |

The 1.x line is maintained on branch `release/1.x` and ships as image tags
`1.0.N`. A 2.0 pre-release is for testing: a fix goes into the next one, not
into a patch of an old one.

## Reporting a vulnerability

Report privately through GitHub's security advisories on this repository, or by
email to security@scadable.com. Include steps to reproduce in plain text.

Please do not report to EMQ; they do not maintain this tree. If the issue is in
code that OpenQTT 1.x and a currently supported EMQX release share, we will tell
them.
