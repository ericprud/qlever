#!/usr/bin/env python3
"""Deliberate-timing dispatcher for Bug #1 (cache-key collision -> 502).

Unlike the barrier-release repro (qlever-update-interleaved-repro.py), this
gives PRECISE, SWEEPABLE control over the delay between an UPDATE landing
and a burst of concurrent identical-key queries firing, instead of relying
on OS thread-scheduling jitter after a simultaneous release.

Two things done to minimize dispatch jitter at the scheduled instant:
  - All connections (update + W query workers) are pre-opened (TCP connect
    done during setup, NOT during the timed release) using low-level
    http.client, so at T0 each worker only does putrequest/endheaders/send.
  - Each worker independently computes its own target = T0 + offset and
    sleeps until then, rather than a single shared Barrier (which only gives
    "simultaneous", not "simultaneous with a controlled offset").

Motivation: a first pass correlating existing [CacheTrace] logs found 2/3
captured collisions at a very short (2-5ms) gap since the previous cache
-clearing update -- well below the p10 of their own baseline gap
distributions. This sweeps the update->query-burst delay to find where the
collision rate actually peaks, so the repro can be tuned instead of relying
on luck.

USAGE
-----
    python3 qlever-timing-sweep-repro.py --url http://localhost:8104 \\
        --delays -10,-5,0,2,5,10,15,20,30,50,100,200 --rounds 50 --width 8
"""
import argparse
import http.client
import threading
import time
import urllib.parse

NS = "http://bench.example/ns#"
ITEM_BASE = "http://bench.example/item/"


def parse_url(url):
    p = urllib.parse.urlparse(url)
    return p.hostname, p.port or 80


def seed_round(host, port, round_no):
    iri = f"{ITEM_BASE}winner-{round_no}"
    preds = " ;\n    ".join(f'ex:p{i} "v{i}"' for i in range(6))
    body = f"PREFIX ex: <{NS}>\nINSERT DATA {{\n  <{iri}> {preds} .\n}}".encode()
    conn = http.client.HTTPConnection(host, port, timeout=10)
    conn.request("POST", "/", body=body,
                 headers={"Content-Type": "application/sparql-update"})
    resp = conn.getresponse()
    resp.read()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"seed failed: {resp.status}")


def lookup_query_body(round_no):
    iri = f"{ITEM_BASE}winner-{round_no}"
    preds = " ;\n          ".join(f"ex:p{i} ?v{i}" for i in range(6))
    return (f"PREFIX ex: <{NS}>\nSELECT * WHERE {{\n  <{iri}> {preds}\n}}\n").encode()


def noop_delete_body():
    return (b"DELETE WHERE { <http://bench.example/item/"
            b"definitely-does-not-exist> ?p ?o }")


class Worker:
    """Pre-connects, then sends its request at a precisely scheduled target
    monotonic time, minimizing per-call jitter from TCP connect overhead."""

    def __init__(self, host, port, method_path, content_type, body):
        self.conn = http.client.HTTPConnection(host, port, timeout=10)
        self.conn.connect()
        self.content_type = content_type
        self.body = body
        self.status = None
        self.text = None
        self.error = None

    def fire_at(self, target_monotonic):
        now = time.monotonic()
        if target_monotonic > now:
            time.sleep(target_monotonic - now)
        try:
            self.conn.putrequest("POST", "/", skip_accept_encoding=True)
            self.conn.putheader("Content-Type", self.content_type)
            self.conn.putheader("Content-Length", str(len(self.body)))
            self.conn.putheader("Accept", "application/sparql-results+json")
            self.conn.endheaders(message_body=self.body)
            resp = self.conn.getresponse()
            self.status = resp.status
            self.text = resp.read().decode("utf-8", errors="replace")
        except Exception as e:  # noqa: BLE001
            self.error = f"{type(e).__name__}: {e}"

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def run_round(host, port, round_no, width, delay_ms):
    """delay_ms > 0: query burst fires AFTER the update by delay_ms.
    delay_ms < 0: query burst fires BEFORE the update (update is delayed)."""
    seed_round(host, port, round_no)

    update_worker = Worker(host, port, "/", "application/sparql-update", noop_delete_body())
    query_workers = [
        Worker(host, port, "/", "application/sparql-query", lookup_query_body(round_no))
        for _ in range(width)
    ]

    lead = 0.15  # seconds of lead time so all threads are sleeping, not still setting up
    t0 = time.monotonic() + lead
    update_target = t0 + max(0.0, -delay_ms / 1000.0)
    query_target = t0 + max(0.0, delay_ms / 1000.0)

    threads = [threading.Thread(target=update_worker.fire_at, args=(update_target,))]
    for w in query_workers:
        threads.append(threading.Thread(target=w.fire_at, args=(query_target,)))
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    codes = [w.status for w in query_workers]
    errors = [w.error for w in query_workers if w.error]
    hit_collision = any(c is not None and c >= 500 for c in codes) or bool(errors)

    update_worker.close()
    for w in query_workers:
        w.close()

    return hit_collision, codes, errors, update_worker.status, update_worker.error


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--delays", required=True,
                     help="comma-separated ms offsets, may include negative, e.g. -10,0,5,20")
    ap.add_argument("--rounds", type=int, default=50, help="rounds per delay value")
    ap.add_argument("--width", type=int, default=8)
    ap.add_argument("--round-offset", type=int, default=0,
                     help="starting round number, to avoid winner-IRI collisions across runs")
    args = ap.parse_args()

    host, port = parse_url(args.url)
    delays = [float(d) for d in args.delays.split(",")]

    round_counter = args.round_offset
    results = {}
    for delay_ms in delays:
        hits = 0
        detail_lines = []
        for _ in range(args.rounds):
            round_counter += 1
            try:
                hit, codes, errors, upd_status, upd_error = run_round(
                    host, port, round_counter, args.width, delay_ms)
            except Exception as e:  # noqa: BLE001
                print(f"  (round {round_counter} setup error, skipping: {e})")
                continue
            if hit:
                hits += 1
                detail_lines.append(
                    f"    round {round_counter}: codes={codes} errors={errors} "
                    f"update_status={upd_status} update_error={upd_error}")
        results[delay_ms] = (hits, args.rounds)
        print(f"delay={delay_ms:+.1f}ms: {hits}/{args.rounds} rounds hit a collision")
        for line in detail_lines[:5]:
            print(line)

    print("\n=== SWEEP SUMMARY ===")
    for delay_ms, (hits, total) in sorted(results.items()):
        rate = 100.0 * hits / total if total else 0.0
        print(f"  delay={delay_ms:+7.1f}ms: {hits:3d}/{total:<4d} ({rate:5.1f}%)")


if __name__ == "__main__":
    raise SystemExit(main())
