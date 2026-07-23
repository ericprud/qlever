#!/usr/bin/env python3
"""Parse [CacheTrace] log lines (from the instrumented ConcurrentCache.h /
Server.cpp) into structured events, then report timing statistics: decision
lock wait time, computeFunction() duration, and end-to-end waiter latency.

Also flags any correctness anomalies seen in the events themselves
(alreadyContained=1, exception traces) regardless of what the surrounding
test run's exit code said.

USAGE
-----
    python3 parse-cachetrace.py <logfile> [<logfile> ...]
"""
import re
import sys
from collections import defaultdict

LINE_RE = re.compile(
    r"\[CacheTrace\] t=(?P<t>[\d,]+) thread=(?P<thread>[\d,]+)"
    r"(?: rip=(?P<rip>\S+))?(?: callId=(?P<callid1>[\d,]+))? (?P<msg>.*)"
)
# `callId=N` embedded inside free-form msg text (e.g. "registered as COMPUTER
# (mustCompute) callId=1,234") -- also comma-formatted by the same locale.
MSG_CALLID_RE = re.compile(r"callId=([\d,]+)")


def parse_int(s):
    return int(s.replace(",", ""))


def load(paths):
    events = []
    for path in paths:
        with open(path, errors="replace") as f:
            for line in f:
                m = LINE_RE.search(line)
                if not m:
                    continue
                callid = m["callid1"]
                if callid is None:
                    msg_match = MSG_CALLID_RE.search(m["msg"])
                    if msg_match:
                        callid = msg_match.group(1)
                events.append({
                    "t": parse_int(m["t"]),
                    "thread": m["thread"],
                    "rip": m["rip"],
                    "callid": callid.replace(",", "") if callid else None,
                    "msg": m["msg"],
                })
    events.sort(key=lambda e: e["t"])
    return events


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    return values[f] + (values[c] - values[f]) * (k - f)


def report_durations(label, values_ns):
    if not values_ns:
        print(f"  {label}: (no samples)")
        return
    us = [v / 1000 for v in values_ns]
    print(f"  {label}: n={len(us)} "
          f"p50={percentile(us,0.5):.1f}us p90={percentile(us,0.9):.1f}us "
          f"p99={percentile(us,0.99):.1f}us max={max(us):.1f}us")


def main():
    paths = sys.argv[1:]
    if not paths:
        print(__doc__)
        return 1
    events = load(paths)
    print(f"Loaded {len(events)} [CacheTrace] events from {len(paths)} file(s).\n")

    # --- anomalies, regardless of exit code ---
    anomalies = [e for e in events if "alreadyContained=1" in e["msg"]
                 or "caught exception" in e["msg"] or "caught non-std" in e["msg"]
                 or "threw:" in e["msg"]]
    print(f"Anomalies found in trace: {len(anomalies)}")
    for e in anomalies[:20]:
        print(f"  t={e['t']} thread={e['thread']} rip={e['rip']} callid={e['callid']} {e['msg']}")
    print()

    # --- per-callId: ENTER -> decision lock acquired (lock wait) ---
    enter_t = {}
    lockwait = []
    for e in events:
        if e["callid"] is None:
            continue
        if e["msg"].startswith("ENTER"):
            enter_t[e["callid"]] = e["t"]
        elif e["msg"] == "decision lock acquired" and e["callid"] in enter_t:
            lockwait.append(e["t"] - enter_t.pop(e["callid"]))

    # --- per-rip: COMPUTER computeFunction() starting -> returned ---
    compute_start = {}
    compute_dur = []
    for e in events:
        if e["rip"] is None:
            continue
        if e["msg"] == "COMPUTER: computeFunction() starting":
            compute_start[e["rip"]] = e["t"]
        elif e["msg"] == "COMPUTER: computeFunction() returned" and e["rip"] in compute_start:
            compute_dur.append(e["t"] - compute_start.pop(e["rip"]))

    # --- per-rip: WAITER about to block -> getResult() returned/threw ---
    waiter_start = {}
    waiter_dur = []
    for e in events:
        if e["rip"] is None:
            continue
        if e["msg"].startswith("WAITER: about to block"):
            waiter_start[e["rip"]] = e["t"]
        elif e["msg"] in ("WAITER: getResult() returned",) or e["msg"].startswith("WAITER: getResult() threw"):
            if e["rip"] in waiter_start:
                waiter_dur.append(e["t"] - waiter_start.pop(e["rip"]))

    # --- update -> clearCache window ---
    update_events = [e for e in events if e["msg"].startswith("UPDATE:")]
    update_windows = []
    pending_start = None
    for e in update_events:
        if "executeUpdate() starting" in e["msg"]:
            pending_start = e["t"]
        elif "clearAll() returned" in e["msg"] and pending_start is not None:
            update_windows.append(e["t"] - pending_start)
            pending_start = None

    n_computer = sum(1 for e in events if "registered as COMPUTER" in e["msg"])
    n_waiter = sum(1 for e in events if "registered as WAITER" in e["msg"])
    print(f"Registrations: {n_computer} computer, {n_waiter} waiter\n")

    print("Timing (all in microseconds):")
    report_durations("decision-lock wait (ENTER -> acquired)", lockwait)
    report_durations("computeFunction() duration (COMPUTER)", compute_dur)
    report_durations("waiter latency (about-to-block -> getResult returns/throws)", waiter_dur)
    report_durations("update execute+clearCache window", update_windows)
    return 0 if not anomalies else 1


if __name__ == "__main__":
    raise SystemExit(main())
