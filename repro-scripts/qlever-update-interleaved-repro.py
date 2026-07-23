#!/usr/bin/env python3
"""Fires an UPDATE (which triggers `cache().clearAll()`) genuinely
concurrently with a burst of byte-identical SELECT queries -- the shape of
the real captured failure: a `DELETE WHERE {...}` landed ~130ms before
several concurrent "losers" independently resolved and looked up the same
winning row via an identical SELECT.

Also captures the server's own [CacheTrace] log for the round, via `docker
logs --since/--until` (if --container is given) so timing can be correlated
directly instead of inferred.

USAGE
-----
    python3 qlever-update-interleaved-repro.py [--url http://localhost:7051]
        [--rounds 300] [--width 6] [--container qlever-homebuilt-instrumented]
        [--log-dir /tmp/...]
"""
import argparse
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

NS = "http://bench.example/ns#"
ITEM_BASE = "http://bench.example/item/"


@dataclass
class Result:
    ok: bool
    status: int
    body: str
    elapsed_s: float


def _http(url: str, data: bytes, content_type: str, accept: str | None) -> Result:
    headers = {"Content-Type": content_type}
    if accept:
        headers["Accept"] = accept
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return Result(True, resp.status, resp.read().decode("utf-8", errors="replace"),
                          time.monotonic() - start)
    except urllib.error.HTTPError as e:
        return Result(False, e.code, e.read().decode("utf-8", errors="replace"),
                      time.monotonic() - start)
    except Exception as e:  # noqa: BLE001
        return Result(False, -1, f"{type(e).__name__}: {e}", time.monotonic() - start)


def sparql_update(url: str, update: str) -> Result:
    return _http(url, update.encode(), "application/sparql-update", None)


def sparql_query(url: str, query: str) -> Result:
    return _http(url, query.encode(), "application/sparql-query",
                 "application/sparql-results+json")


def seed_round(url: str, round_no: int, width: int) -> None:
    """One 'winner' row per round (fresh IRI, never reused), so every
    round's identical lookup queries are for a subject that has never
    appeared before -- matching the real scenario (a fresh ULID per rule)."""
    iri = f"{ITEM_BASE}winner-{round_no}"
    preds = " ;\n    ".join(f'ex:p{i} "v{i}"' for i in range(6))
    r = sparql_update(url, f"PREFIX ex: <{NS}>\nINSERT DATA {{\n  <{iri}> {preds} .\n}}")
    if not r.ok:
        raise RuntimeError(f"seed failed: {r.status} {r.body}")


def lookup_query(round_no: int) -> str:
    iri = f"{ITEM_BASE}winner-{round_no}"
    preds = " ;\n          ".join(f"ex:p{i} ?v{i}" for i in range(6))
    return f"PREFIX ex: <{NS}>\nSELECT * WHERE {{\n  <{iri}> {preds}\n}}\n"


def run_round(url: str, round_no: int, width: int) -> tuple[list[Result], Result]:
    """Fires a DELETE WHERE {} update and `width` identical lookups for this
    round's winner subject, all starting from essentially the same instant.
    """
    query = lookup_query(round_no)
    results: list[Result | None] = [None] * width
    update_result: list[Result | None] = [None]

    barrier = threading.Barrier(width + 1)

    def query_worker(i: int) -> None:
        barrier.wait()
        results[i] = sparql_query(url, query)

    def update_worker() -> None:
        barrier.wait()
        # A DELETE WHERE that matches nothing (the winner row must survive
        # for this round's lookups) but still goes through the exact same
        # executeUpdate()+clearCache() path a real `DELETE WHERE {?s ?p ?o}`
        # full wipe would.
        update_result[0] = sparql_update(
            url, f"DELETE WHERE {{ <{ITEM_BASE}definitely-does-not-exist> ?p ?o }}")

    threads = [threading.Thread(target=query_worker, args=(i,)) for i in range(width)]
    threads.append(threading.Thread(target=update_worker))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, update_result[0]


def container_logs(container: str, since_ns: float, until_ns: float) -> str:
    since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(since_ns))
    until = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(until_ns + 1))
    try:
        out = subprocess.run(
            ["docker", "logs", container, "--since", since, "--until", until],
            capture_output=True, text=True, timeout=15,
        )
        return out.stdout + out.stderr
    except Exception as e:  # noqa: BLE001
        return f"(failed to fetch container logs: {e})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://localhost:7051")
    ap.add_argument("--rounds", type=int, default=300)
    ap.add_argument("--width", type=int, default=6)
    ap.add_argument("--container", default=None,
                     help="docker container name to pull [CacheTrace] logs from on anomaly")
    ap.add_argument("--log-file", default=None,
                     help="if set, append every round's [CacheTrace]-relevant timing to this file")
    args = ap.parse_args()

    print(f"Target: {args.url}  container={args.container}")
    r = sparql_query(args.url, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")
    if not r.ok:
        print(f"Endpoint not reachable: status={r.status} body={r.body}", file=sys.stderr)
        return 2

    for round_no in range(1, args.rounds + 1):
        seed_round(args.url, round_no, args.width)
        t_before = time.time()
        results, update_result = run_round(args.url, round_no, args.width)
        t_after = time.time()

        bad = [(i, r) for i, r in enumerate(results) if not r.ok]
        update_bad = update_result is not None and not update_result.ok
        if bad or update_bad:
            print(f"round {round_no:4d}/{args.rounds}: ANOMALY")
            print("=" * 70)
            for i, r in bad:
                print(f"query {i}: status={r.status} elapsed={r.elapsed_s:.3f}s body={r.body[:500]}")
            if update_bad:
                print(f"update: status={update_result.status} "
                      f"elapsed={update_result.elapsed_s:.3f}s body={update_result.body[:500]}")
            if args.container:
                print("--- container [CacheTrace] log for this round ---")
                print(container_logs(args.container, t_before - 1, t_after + 1))
            print("=" * 70)
            return 1

        if args.log_file:
            with open(args.log_file, "a") as f:
                f.write(f"{round_no}\t{t_before}\t{t_after}\n")

        if round_no % 20 == 0:
            print(f"round {round_no:4d}/{args.rounds}: clean")

    print(f"\nAll {args.rounds} rounds clean -- did not reproduce this run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
