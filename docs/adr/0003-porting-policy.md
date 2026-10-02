# ADR 0003: Porting policy

- Status: accepted
- Date: 2026-10-01

## Context

2.0 replaces a broker whose behaviour is known to work: EMQX 5.8.9, the source
of OpenQTT 1.x. Reading it is the fastest way to learn the corners of MQTT 5
that the specification leaves open. It is also how code with the wrong licence,
or the right licence and no attribution, ends up in an Apache 2.0 repository.
This policy says what may be read, what may be taken, and how a taken thing is
marked.

## Decision

1. **Implement from the specification.** The OASIS MQTT 5.0 standard is the
   default source for every behaviour. Code that implements a normative
   statement names it, for example `[MQTT-3.1.2-2]`.
2. **Cite EMQX, do not paste it.** When EMQX's behaviour informs a decision,
   link to it at the import tag, never at a branch:
   `https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/<path>#L<from>-L<to>`.
   Tag `emqx-v5.8.9` is upstream commit
   `a8319fe2390169e1f2483e3ec80dd01a6cdb233d`, so a link to that commit in
   `emqx/emqx` is equivalent. Where EMQX and the specification disagree, the
   specification wins unless report R1 records the divergence.
3. **Mark what is derived.** A file that translates EMQX logic, or copies or
   adapts its tests, starts with this line, with `<path>` the source file
   relative to the EMQX tree:

   ```rust
   // Portions derived from EMQX 5.8.9 (<path>), Copyright (c) 2017-2025 EMQ Technologies Co., Ltd., Apache-2.0. Modified by SCADABLE IoT.
   ```

   One line per source file. A format without comments carries the line in a
   README beside it. `NOTICE` covers every file marked this way.
4. **Never read or port EMQX 5.9 or later**, or any upstream directory that
   carries a `BSL.txt`, at any version. That code is under the Business Source
   License, and having read it is enough to taint a clean implementation. The
   import tag contains no such directory; upstream's tree at the same commit
   does.
5. **Never vendor Eclipse Paho test code.** It is under the EPL and EDL. Restate
   a Paho scenario in our own words and code, and name it so a reader can find
   the original.
6. **Anything else** comes in only under a licence compatible with
   distribution under Apache 2.0, with its notices kept. Dependencies are
   checked by cargo-deny on every pull request.

## Consequences

- A reviewer can tell a derived file from its first line, and `NOTICE` stays
  true without a file-by-file audit.
- Reading EMQX at the import tag is encouraged. Reading it anywhere newer is
  not allowed.
- A test ported from EMQX carries the same header as ported logic.
