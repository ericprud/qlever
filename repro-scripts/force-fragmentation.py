#!/usr/bin/env python3
"""Deliberately fragments QLever's delta-triples overlay by firing many
small, separate INSERT DATA updates (one per call) instead of a few large
bulk loads -- matching the pattern that produced the originally-observed
353ms query time / 12.2% collision rate (built up incidentally over many
small test-round inserts), rather than the fast 17-20ms bulk-reload state.

Periodically checks a fixed probe query's timing so progress toward
re-fragmentation can be watched directly.

USAGE: force-fragmentation.py --url http://localhost:8104 --count 5000 --check-every 500
"""
import argparse
import http.client
import json
import time
import urllib.parse

NS = "http://bench.example/ns#"

_conn = {}


def _get_conn(url):
    if "c" not in _conn:
        p = urllib.parse.urlparse(url)
        _conn["c"] = http.client.HTTPConnection(p.hostname, p.port or 80, timeout=15)
        _conn["c"].connect()
    return _conn["c"]


def post(url, key, body, timeout=15):
    data = urllib.parse.urlencode({key: body}).encode()
    conn = _get_conn(url)
    try:
        conn.request("POST", "/", body=data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json",
        })
        resp = conn.getresponse()
        return resp.read()
    except (http.client.HTTPException, ConnectionError, BrokenPipeError):
        _conn.pop("c", None)
        conn = _get_conn(url)
        conn.request("POST", "/", body=data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json",
        })
        resp = conn.getresponse()
        return resp.read()


def probe_query_time_ms(url):
    resp = post(url, "query",
                "SELECT * WHERE { <http://bench.example/item/winner-700139> ?p ?o }")
    result = json.loads(resp)
    return result.get("meta", {}).get("query-time-ms")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--count", type=int, default=5000)
    ap.add_argument("--check-every", type=int, default=500)
    ap.add_argument("--start-i", type=int, default=0)
    args = ap.parse_args()

    print("query time before:", probe_query_time_ms(args.url), "ms")

    t0 = time.time()
    for i in range(args.start_i, args.start_i + args.count):
        body = (
            f"INSERT DATA {{ <http://bench.example/frag/{i}> "
            f"<{NS}p0> \"v0\" ; <{NS}p1> \"v1\" ; <{NS}p2> \"v2\" ; "
            f"<{NS}p3> \"v3\" ; <{NS}p4> \"v4\" ; <{NS}p5> \"v5\" . }}"
        )
        post(args.url, "update", body)
        if (i - args.start_i + 1) % args.check_every == 0:
            qt = probe_query_time_ms(args.url)
            elapsed = time.time() - t0
            print(f"after {i - args.start_i + 1} fragments "
                  f"({elapsed:.1f}s elapsed): query-time={qt}ms")

    print("DONE. final query time:", probe_query_time_ms(args.url), "ms")


if __name__ == "__main__":
    raise SystemExit(main())
