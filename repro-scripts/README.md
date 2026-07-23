# Cache-key-collision (Bug #1) reproduction & analysis scripts

Companion scripts for the fix in `src/util/ConcurrentCache.h`
(`moveFromInProgressToCache`), which resolves an intermittent
`"Trying to insert a cache key which was already present"` /
`WaitedForResultWhichThenFailedException` error under concurrent load. See
that commit's message for the full root-cause writeup.

All scripts are plain Python 3 (stdlib only) or bash; no dependencies beyond
the QLever server itself.

## Reproducing the bug

- **`qlever-timing-sweep-repro.py`** -- the primary, high-throughput
  reproducer. Fires a `DELETE WHERE {...}` (a no-op update that still goes
  through the real `executeUpdate()` + `clearAll()` path) concurrently with
  an N-way burst of identical lookup queries for that round's subject, using
  pre-opened persistent connections and independent `sleep-until(t0 +
  offset)` scheduling per worker (instead of a single `threading.Barrier`
  release) for precise, low-jitter timing control. Supports sweeping the
  update/query-burst delay via `--delays`.
  ```
  python3 qlever-timing-sweep-repro.py --url http://localhost:PORT \
      --delays=0 --rounds 300 --width 8
  ```
  Typical hit rate: 0.3-2% on a small/fresh dataset, up to ~12% on a
  heavily-fragmented one (see below) -- all on the *unfixed* binary.
  0/N on the fixed binary in every run performed during this investigation.

- **`qlever-update-interleaved-repro.py`** -- the earlier,
  `threading.Barrier`-based variant (single simultaneous release, no timing
  control). Lower hit rate but simpler; kept for reference / cross-check.
  Supports `--container` to auto-snapshot `docker logs` on an anomaly.

## Does dataset size / fragmentation matter?

Short answer: **it changes the odds, but is not required to trigger the
bug.** A longer per-query `computeFunction()` duration widens the race
window. Measured on one dataset across three states (same 789,835 triples):
- Clean/small (~3K triples, ~1-6ms queries): 0/500 hit the bug.
- Same triples, bulk-reloaded in 40 large `INSERT DATA` calls (~17-20ms
  queries, low delta-fragmentation): 9/200 (4.5%).
- Same triples, built up via thousands of small incremental updates over
  time (~353ms queries, heavily fragmented delta overlay): 61/500 (12.2%).

- **`force-fragmentation.py`** -- deliberately fragments the delta-triples
  overlay via many small, separate `INSERT DATA` calls (fragmentation is
  driven by *update count*, not update size or total triple volume).
  **Caution**: each single-triple insert costs real server-side time that
  scales with the *existing* dataset size (~0.5-0.6s/insert observed on a
  790K-triple dataset) -- rebuilding a heavily-fragmented state this way is
  slow (potentially an hour+); useful for understanding the effect, not a
  fast way to reproduce.

## Analysis

- **`parse-cachetrace.py`** -- parses `[CacheTrace]` log lines (see the
  instrumentation commit) into structured events; reports lock-wait,
  `computeFunction()` duration, waiter-latency, and update-window
  percentiles, and flags `alreadyContained=1` / exception lines as
  anomalies regardless of the surrounding test run's exit code.
- **`timing-correlation.py`** -- for each captured collision, reports the
  gap since the most recent `cache().clearAll()` and a baseline
  distribution of that gap across all requests, for correlating collisions
  against update timing. (Caveat: this gap metric is only meaningful when
  the racing update predates the query by a clear margin; with
  simultaneous-fire dispatch, as in `qlever-timing-sweep-repro.py` at
  `delay=0`, the "most recent prior clearAll" is usually just the previous
  round's own update, not a causally-relevant signal -- see the commit
  history / investigation notes for how the actual root cause was found via
  the `key:`-content-annotated traces instead.)

## Reproducing against the real application

The original report came from `bpg-reproduce-qlever-error`'s own pytest
suite (`tests/`), run in a loop against a QLever instance with
`SPARQL_URL=http://localhost:PORT ENDPOINTS=http://localhost:PORT uv run
pytest tests/ -v`, looping until a failure (or confirming N clean rounds
against the fixed binary). Not included here since it depends on that
separate application; the synthetic scripts above reproduce the same
underlying QLever bug without any application-specific dependency.
