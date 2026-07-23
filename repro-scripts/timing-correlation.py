#!/usr/bin/env python3
"""For each captured cache-collision instance (alreadyContained=1) in a
[CacheTrace]-instrumented server log, find:
  - the callId/rip that hit the collision, and its own ENTER timestamp
  - the most recent preceding "UPDATE: cache().clearAll() returned" (any
    update bumps the snapshot index and clears the whole cache -- this is
    the most plausible "trigger" event upstream of a collision)
  - the gap (ms) between that update's clearCache-return and the colliding
    call's ENTER
Also samples a baseline of non-colliding ENTER events' "time since last
update" gaps, for comparison, to see whether collisions cluster at a
particular gap.
"""
import re
import sys

LINE_RE = re.compile(
    r"(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) - ERROR: \[CacheTrace\] "
    r"t=(?P<t>[\d,]+) thread=(?P<thread>[\d,]+)"
    r"(?: rip=(?P<rip>\S+))?(?: callId=(?P<callid>[\d,]+))? (?P<msg>.*)"
)


def parse_int(s):
    return int(s.replace(",", ""))


def load(path):
    events = []
    with open(path, errors="replace") as f:
        for line in f:
            m = LINE_RE.search(line)
            if not m:
                continue
            events.append({
                "ts": m["ts"],
                "t": parse_int(m["t"]),
                "thread": m["thread"],
                "rip": m["rip"],
                "callid": m["callid"].replace(",", "") if m["callid"] else None,
                "msg": m["msg"],
            })
    events.sort(key=lambda e: e["t"])
    return events


def analyze(path):
    events = load(path)
    print(f"\n=== {path} ({len(events)} CacheTrace lines) ===")

    # last clearAll() timestamp seen so far, and ENTER-time per callId
    last_clear_t = None
    enter_t = {}
    rip_to_callid_enter = {}  # rip -> (callid, enter_t) for the most recent
                              # "registered as COMPUTER/WAITER" on that rip
    clear_gaps_all = []  # (t_of_enter, gap_ms) for every ENTER, for baseline

    collisions = []

    for e in events:
        msg = e["msg"]
        if msg.startswith("ENTER"):
            enter_t[e["callid"]] = e["t"]
            if last_clear_t is not None:
                gap_ms = (e["t"] - last_clear_t) / 1e6
                clear_gaps_all.append(gap_ms)
        elif "registered as COMPUTER" in msg or "registered as WAITER" in msg:
            if e["rip"] and e["callid"] in enter_t:
                rip_to_callid_enter[e["rip"]] = (e["callid"], enter_t[e["callid"]])
        elif "cache().clearAll() returned" in msg:
            last_clear_t = e["t"]
        elif "alreadyContained=1" in msg:
            rip = e["rip"]
            callid, enter = rip_to_callid_enter.get(rip, (None, None))
            gap_ms = (e["t"] - last_clear_t) / 1e6 if last_clear_t is not None else None
            collisions.append({
                "ts": e["ts"], "rip": rip, "callid": callid,
                "collision_t": e["t"], "enter_t": enter,
                "gap_since_last_clear_ms": gap_ms,
            })

    print(f"Collisions found: {len(collisions)}")
    for c in collisions:
        print(f"  ts={c['ts']} rip={c['rip']} callid={c['callid']} "
              f"gap_since_last_clearAll={c['gap_since_last_clear_ms']:.3f}ms"
              if c['gap_since_last_clear_ms'] is not None else
              f"  ts={c['ts']} rip={c['rip']} callid={c['callid']} gap=None (no prior clearAll seen)")

    if clear_gaps_all:
        gaps_sorted = sorted(clear_gaps_all)
        n = len(gaps_sorted)
        print(f"Baseline: {n} ENTER events, gap-since-last-clearAll distribution (ms):")
        for p in (0, 10, 25, 50, 75, 90, 99, 100):
            idx = min(int(n * p / 100), n - 1)
            print(f"    p{p}: {gaps_sorted[idx]:.3f}")
    return collisions, clear_gaps_all


def main():
    paths = sys.argv[1:]
    if not paths:
        print(__doc__)
        return 1
    all_collisions = []
    for p in paths:
        c, _ = analyze(p)
        all_collisions.extend(c)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
