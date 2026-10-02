#!/usr/bin/env python3
"""Folds every out/<step>.jsonl into one JSON document under bench/results.

    python3 collect.py [date]

The date names the file (s1-<date>.json) and defaults to today. Each record keeps every field
the bench wrote; the document adds the machine, the versions under test and the caveats that
apply to all of them.
"""

import datetime
import json
import pathlib
import platform
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def sh(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return None


def machine():
    m = {"os": platform.platform(), "arch": platform.machine()}
    if sys.platform == "darwin":
        m["cpu"] = sh("sysctl", "-n", "machdep.cpu.brand_string")
        m["cores"] = sh("sysctl", "-n", "hw.ncpu")
        m["performance_cores"] = sh("sysctl", "-n", "hw.perflevel0.physicalcpu")
        m["efficiency_cores"] = sh("sysctl", "-n", "hw.perflevel1.physicalcpu")
        mem = sh("sysctl", "-n", "hw.memsize")
        m["memory_gib"] = int(mem) / 2**30 if mem else None
        m["macos"] = sh("sw_vers", "-productVersion")
        m["filesystem"] = "APFS on the internal SSD"
    m["rustc"] = sh("rustc", "--version")
    return m


def versions():
    lock = (HERE / "Cargo.lock").read_text()
    want = {"fjall", "lsm-tree", "redb", "rocksdb", "librocksdb-sys", "openraft", "tokio"}
    found = {}
    name = None
    for line in lock.splitlines():
        if line.startswith("name = "):
            name = line.split('"')[1]
        elif line.startswith("version = ") and name in want:
            found.setdefault(name, []).append(line.split('"')[1])
    return found


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    runs = {}
    for f in sorted((HERE / "out").glob("*.jsonl")):
        runs[f.stem] = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
    doc = {
        "spike": "S1",
        "report": "docs/reports/R04-log.md",
        "date": date,
        "machine": machine(),
        "versions": versions(),
        "caveats": [
            "Laptop numbers: one APFS SSD shared by every simulated node, macOS, and other "
            "work running on the machine (load average 4 to 20 during the runs, recorded per "
            "record as load_avg). Compare candidates and shapes, not absolute values.",
            "Durable means F_FULLFSYNC on macOS for all three engines (RocksDB built with "
            "-DHAVE_FULLFSYNC). Production Linux uses fdatasync.",
            "Replication runs three replicas in one process over a simulated network and a "
            "modelled per-node flush; message CPU excludes serialisation, sockets and TLS.",
            "Latencies are microseconds. Open-loop latencies are measured from when each "
            "request was due, so queueing behind a stall is counted.",
        ],
        "runs": runs,
    }
    out = ROOT / "bench" / "results" / f"s1-{date}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n")
    print(f"{out}: {sum(len(v) for v in runs.values())} records from {len(runs)} steps")


if __name__ == "__main__":
    main()
