#!/bin/sh
# Lists the MQTT 5.0 statements in report R1 that no test names yet, so that conformance
# coverage can be read off the test tree rather than kept by hand.
#
#   scripts/conformance-ids.sh [REPORT [DIR...]]
#
# Paths are relative to the repository root. REPORT defaults to
# docs/reports/R01-conformance.md and DIR to crates. The statement ids are the first cell of
# the report's table rows. A test names a statement in its function name,
# `fn mqtt_<x>_<y>_<z>_<n>_<what it checks>`, or on a line containing `covers:` followed by
# ids written MQTT-x.y.z-n; R1 describes the convention. A citation such as [MQTT-3.1.2-2] in
# implementation code is not a test, so it is not counted.
#
# Prints the untested ids on stdout, one per line in the report's order, and a count on
# stderr. Fails when the report lists no ids, or when a test names an id the report does not
# list, which is a typo in that test's name.
set -eu

cd "$(dirname "$0")/.."

report=${1:-docs/reports/R01-conformance.md}
if [ $# -gt 0 ]; then shift; fi
if [ $# -eq 0 ]; then set -- crates; fi

id='MQTT-[0-9]+\.[0-9]+\.[0-9]+-[0-9]+'

ids=$(grep -oE "^[|] $id [|]" "$report" | grep -oE "$id" || true)
if [ -z "$ids" ]; then
    echo "error: $report lists no statement ids" >&2
    exit 1
fi

# Every id a test names, from test function names and from covers: lines.
tagged=$(
    {
        grep -rhoIE 'fn mqtt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[a-z]' --include='*.rs' "$@" |
            sed -E 's/^fn mqtt_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)_[a-z]$/MQTT-\1.\2.\3-\4/'
        grep -rhIE 'covers:' "$@" | grep -oE "$id"
    } 2>/dev/null | sort -u
)

untested=$(printf '%s\n' "$ids" | TAGGED="$tagged" awk '
    BEGIN { n = split(ENVIRON["TAGGED"], t, "\n"); for (i = 1; i <= n; i++) seen[t[i]] = 1 }
    !($0 in seen)')
unknown=$(printf '%s\n' "$tagged" | IDS="$ids" awk '
    BEGIN { n = split(ENVIRON["IDS"], r, "\n"); for (i = 1; i <= n; i++) known[r[i]] = 1 }
    NF && !($0 in known)')

if [ -n "$untested" ]; then
    printf '%s\n' "$untested"
fi
total=$(printf '%s\n' "$ids" | wc -l | tr -d ' ')
open=$(printf '%s' "$untested" | grep -c . || true)
echo "conformance-ids: $open of $total statements in $report have no test" >&2

if [ -n "$unknown" ]; then
    echo "error: tests name statements $report does not list:" >&2
    printf '%s\n' "$unknown" >&2
    exit 1
fi
