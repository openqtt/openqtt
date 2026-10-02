# Spikes

Measurements made before the code they inform. Each spike is a crate and workspace of its own,
excluded from the product workspace, with every dependency at an exact version and its own lock
file, so nothing a spike pulls in reaches the broker. A spike's `run.sh` writes the JSON its
report quotes into `bench/results/`.

| Spike | Question | Report |
| --- | --- | --- |
| [`s1-log`](s1-log) | The log role's storage engine, replication scheme, group commit and shared fsync | [R4](../docs/reports/R04-log.md) |
| [`s2-routing`](s2-routing) | Router memory, match latency, fan-out, interest coarsening and route-view churn | [R6](../docs/reports/R06-routing.md) |
| [`s4-quinn`](s4-quinn) | Memory and CPU per idle QUIC connection, handshake rates, transport settings | [R7](../docs/reports/R07-quic.md), section S4 |

The numbers are laptop numbers: each report says which machine and how to read them.
