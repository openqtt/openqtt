#!/usr/bin/env python3
"""Prints the Markdown tables report R4 quotes, from out/*.jsonl (or a collected results file).

    python3 summarize.py [results.json]

Where a configuration ran more than once (the write and shared steps run three passes), the
table shows the median of the passes and the spread of the p99 as min to max.
"""

import json
import pathlib
import statistics
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent


def load():
    if len(sys.argv) > 1:
        return json.loads(pathlib.Path(sys.argv[1]).read_text())["runs"]
    runs = {}
    for f in sorted((HERE / "out").glob("*.jsonl")):
        runs[f.stem] = [json.loads(l) for l in f.read_text().splitlines() if l.strip()]
    return runs


def ms(us):
    if us is None:
        return "-"
    return f"{us / 1000:.1f}" if us < 100_000 else f"{us / 1000:.0f}"


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def k(n):
    if n is None:
        return "-"
    return f"{n / 1000:.1f}k" if n >= 1000 else f"{n:.0f}"


def mib(b):
    return f"{b / 2**20:.0f}"


def fsync(rows):
    print("\n### Durability floor (one thread unless noted)\n")
    print("| Primitive | File | Size | Threads | Syncs/s | p50 ms | p99 ms |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        L = r["lat_us"]
        print(f"| {r['primitive']} | {r['mode'].lower()} | {r['size']} | {r['threads']} | "
              f"{r['per_s']:.0f} | {ms(L['p50'])} | {ms(L['p99'])} |")


def grouped(rows, keys):
    g = defaultdict(list)
    for r in rows:
        g[tuple(r[k_] for k_ in keys)].append(r)
    return g


def write(rows):
    g = grouped(rows, ["size", "window_us", "load", "engine"])
    print("\n### Group commit, window 0 (median of passes; p99 range across passes)\n")
    print("| Size | Load | Engine | Writes/s | p50 ms | p99 ms | p99.9 ms | p99 range ms | Batch | Write amp |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for (size, w, load, eng), rs in sorted(g.items(), key=lambda x: (x[0][0], x[0][1], order_load(x[0][2]), x[0][3])):
        if w != 0:
            continue
        print(row_write(size, load, eng, rs))
    print("\n### Group commit window, 128 B (median of passes)\n")
    print("| Load | Engine | 0 ms p50/p99 | 1 ms p50/p99 | 2 ms p50/p99 |")
    print("| --- | --- | --- | --- | --- |")
    for load in ["closed:1", "closed:64", "open:10000/s", "open:50000/s"]:
        for eng in ["fjall", "redb", "rocksdb"]:
            cells = []
            for w in [0, 1000, 2000]:
                rs = g.get((128, w, load, eng), [])
                p50 = med([r["lat_us"]["p50"] for r in rs if r["lat_us"]])
                p99 = med([r["lat_us"]["p99"] for r in rs if r["lat_us"]])
                cells.append(f"{ms(p50)} / {ms(p99)}")
            print(f"| {load} | {eng} | " + " | ".join(cells) + " |")


def order_load(l):
    kind, n = l.split(":")
    return (0 if kind == "closed" else 1, int(n.split("/")[0]))


def row_write(size, load, eng, rs):
    L = [r["lat_us"] for r in rs if r["lat_us"]]
    p50 = med([x["p50"] for x in L])
    p99s = [x["p99"] for x in L]
    p999 = med([x["p999"] for x in L])
    per_s = med([r["per_s"] for r in rs])
    batch = med([r["commit"]["mean_batch"] for r in rs])
    amp = med([r["disk_written"] / max(1, r["commit"]["logical_bytes"]) for r in rs])
    return (f"| {size} | {load} | {eng} | {k(per_s)} | {ms(p50)} | {ms(med(p99s))} | {ms(p999)} | "
            f"{ms(min(p99s))} to {ms(max(p99s))} | {batch:.0f} | {amp:.1f} |")


def shared(rows):
    g = grouped(rows, ["load", "shape", "engine"])
    print("\n### One fsync for the log and the state machine, or two (median of passes)\n")
    print("| Load | Shape | Engine | Writes/s | p50 ms | p99 ms | Engine write p50 ms | Write amp |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for (load, shape, eng), rs in sorted(g.items(), key=lambda x: (order_load(x[0][0]), x[0][1], x[0][2])):
        L = [r["lat_us"] for r in rs if r["lat_us"]]
        ew = med([r["commit"]["engine_write_us"]["p50"] for r in rs if r["commit"]["engine_write_us"]])
        amp = med([r["disk_written"] / max(1, r["commit"]["logical_bytes"]) for r in rs])
        print(f"| {load} | {shape} | {eng} | {k(med([r['per_s'] for r in rs]))} | "
              f"{ms(med([x['p50'] for x in L]))} | {ms(med([x['p99'] for x in L]))} | {ms(ew)} | {amp:.1f} |")


def claims(rows):
    print("\n### Claim storm on one node's engine\n")
    print("| Engine | Workers | Offered/s | Done/s | p50 ms | p99 ms | p99.9 ms | Read p50 us | Batch | CPU cores |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        if r["exp"] == "claims-preload":
            continue
        L = r["lat_us"] or {}
        R = r["read_us"] or {}
        print(f"| {r['engine']} | {r['workers']} | {k(r['rate'])} | {k(r['per_s'])} | {ms(L.get('p50'))} | "
              f"{ms(L.get('p99'))} | {ms(L.get('p999'))} | {R.get('p50', '-')} | "
              f"{r['commit']['mean_batch']:.0f} | {r['cpu_cores']} |")
    for r in rows:
        if r["exp"] == "claims-preload":
            print(f"\npreload {r['engine']}: {r['sessions']} sessions in {r['secs']} s, "
                  f"{mib(r['disk_allocated'])} MiB")


def footprint(rows):
    loads = {(r["engine"], r["sessions"]): r for r in rows if r["exp"] == "footprint-load"}
    idles = {(r["engine"], r["sessions"]): r for r in rows if r["exp"] == "footprint-idle"}
    print("\n### Footprint per idle session (own plus sess)\n")
    print("| Engine | Sessions | Load s | Disk B/session (loaded) | Disk B/session (compacted) | "
          "RSS settled MiB | RSS B/session | Footprint B/session | Open s |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for key in sorted(loads):
        l = loads[key]
        i = idles.get(key, {})
        rss = i.get("rss", {}).get("settled")
        print(f"| {key[0]} | {key[1]:,} | {l['load_s']} | {l['loaded']['per_session']} | "
              f"{l['compacted']['per_session']} | {mib(rss) if rss else '-'} | "
              f"{i.get('rss_per_session', '-')} | {i.get('footprint_per_session', '-')} | {i.get('open_s', '-')} |")
    if loads:
        print(f"\nlogical bytes per session: {next(iter(loads.values()))['logical_bytes'] / next(iter(loads.values()))['sessions']:.0f}")


def churn(rows):
    print("\n### Queue churn\n")
    print("| Engine | Secs | Published | Write p50 ms | p99 ms | p99.9 ms | max ms | Worst second p99 ms | "
          "Stall s | Drain p99 ms | Scan p99 ms | GC p99 ms | Disk end MiB | Disk max MiB | CPU |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        w = r["write_us"]
        worst = max(r["series"]["write_p99"]) if r["series"]["write_p99"] else None
        dmax = max(d["allocated"] for d in r["disk"]) if r["disk"] else 0
        print(f"| {r['engine']} | {r['secs']} | {r['published']:,} | {ms(w['p50'])} | {ms(w['p99'])} | "
              f"{ms(w['p999'])} | {ms(w['max'])} | {ms(worst)} | {r['stall_seconds']} | "
              f"{ms((r['drain_us'] or {}).get('p99'))} | {ms((r['scan_us'] or {}).get('p99'))} | "
              f"{ms((r['gc_us'] or {}).get('p99'))} | {mib(r['final_disk']['allocated'])} | {mib(dmax)} | {r['cpu_cores']} |")


def recovery(rows):
    print("\n### Recovery\n")
    for r in rows:
        e = r["exp"]
        if e == "recovery-load":
            print(f"- load {r['engine']} {r['gib']} GiB quick_repair={r['quick_repair']}: {r['load_s']} s, "
                  f"{r['allocated'] / 2**30:.1f} GiB on disk")
        elif e == "recovery-open":
            print(f"- open {r['engine']} {r['label']} quick_repair={r['quick_repair']}: open {r['open_s']} s, "
                  f"ready {r['ready_s']} s, found={r['found']}")
        elif e == "recovery-replay":
            print(f"- replay {r['engine']}: {r['entries']:,} entries, append {r['append_s']} s, "
                  f"replay {r['replay_s']} s ({k(r['replay_per_s'])}/s), read {r['read_s']} s")


def repl(rows):
    print("\n### Replication\n")
    print("| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | Errors | "
          "Msgs/s | Items/flush | CPU cores | Notes |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        R = r["result"]
        L = R["lat_us"] or {}
        note = ""
        ex = r.get("extra", {})
        if ex.get("misplaced_leaders"):
            note = f"{ex['misplaced_leaders']} leaders moved"
        print(f"| {r['scheme']} | {r['groups']} | {r['delay_us'] / 1000:g} | {r['flush_us'] / 1000:g} | {r['load']} | "
              f"{k(R['per_s'])} | {ms(L.get('p50'))} | {ms(L.get('p99'))} | {R['errors']} | "
              f"{k(R['messages_per_s'])} | {R['disk_items_per_flush']} | {R['cpu_cores_total']} | {note} |")


def idle(rows):
    print("\n### Idle cost\n")
    print("| Scheme | Groups | Heartbeat ms | Election ms | CPU cores, 3 nodes | Per node | "
          "us/s per group replica | Messages/s |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in rows:
        print(f"| {r['scheme']} | {r['groups']} | {r['heartbeat_ms']} | {r['election_ms'][0]} to {r['election_ms'][1]} | "
              f"{r['cpu_cores_total']} | {r['cpu_cores_per_node']} | {r['cpu_us_per_group_replica_per_s']} | "
              f"{k(r['messages_per_s'])} |")


def build(rows):
    print("\n### Build friction\n")
    base = {(r["where"], r["profile"]): r for r in rows if r["feature"] == "none"}
    print("| Where | Feature | Profile | Clean build s | Binary MiB | Added MiB |")
    print("| --- | --- | --- | --- | --- | --- |")
    for r in rows:
        b = base.get((r["where"], r["profile"]))
        added = (r["binary_bytes"] - b["binary_bytes"]) / 2**20 if b else 0
        print(f"| {r['where']} | {r['feature']} | {r['profile']} | {r['secs']} | "
              f"{r['binary_bytes'] / 2**20:.1f} | {added:.1f} |")


def main():
    runs = load()
    for step, fn in [("fsync", fsync), ("write", write), ("shared", shared), ("claims", claims),
                     ("footprint", footprint), ("churn", churn), ("recovery", recovery),
                     ("repl", repl), ("repl-storm", repl), ("repl-max", repl), ("idle", idle),
                     ("build", build)]:
        if step in runs:
            print(f"\n## {step}")
            fn(runs[step])


if __name__ == "__main__":
    main()
