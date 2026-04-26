#!/usr/bin/env bash
#
# run-complete.sh — drive the `benchmark.jmh.complete.*` JMH suite.
#
# Modes:
#   fast   ~5–10 minutes total: 1 warmup + 2 measurement iterations × 3s, recordCount=5_000,
#          a small backend subset that still touches every category. Use this to validate the
#          suite runs end-to-end before kicking off a long run.
#   full   The exhaustive run (designed for overnight): 5 warmup × 10s + 5 measurement × 10s
#          per benchmark, recordCount=100_000, every backend × every benchmark × every thread
#          count (1 / 4 / 16) for the concurrency suite. Plan on 6–14 hours depending on disk
#          speed.
#
# Output (per pass; the script makes 2 sub-runs and merges nothing — one JSON per state class):
#   benchmark/target/jmh-results-<mode>-<timestamp>-kv.json     # all-backends pass
#   benchmark/target/jmh-results-<mode>-<timestamp>-coll.json   # collection-only pass
#   benchmark/target/jmh-results-<mode>-<timestamp>.log         # combined console log
#
# Usage:
#   ./benchmark/run-complete.sh fast
#   ./benchmark/run-complete.sh full
#   ./benchmark/run-complete.sh full -p backend=lucene,tantivy   # extra args appended to BOTH passes
#
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <fast|full> [extra jmh args]" >&2
  exit 2
fi

MODE="$1"; shift || true
EXTRA_ARGS=("$@")

case "$MODE" in
  fast|full) ;;
  *) echo "unknown mode: $MODE (expected: fast | full)" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$REPO_ROOT"

mkdir -p benchmark/target
TS="$(date +%Y%m%d-%H%M%S)"
# JMH resolves the result file relative to its forked process cwd, which sbt sets unpredictably
# — always pass an absolute path so the file lands where we expect.
RESULT_KV="$REPO_ROOT/benchmark/target/jmh-results-${MODE}-${TS}-kv.json"
RESULT_COLL="$REPO_ROOT/benchmark/target/jmh-results-${MODE}-${TS}-coll.json"
LOG_FILE="$REPO_ROOT/benchmark/target/jmh-results-${MODE}-${TS}.log"

# --- mode-specific knobs --------------------------------------------------------------------

if [[ "$MODE" == "fast" ]]; then
  WARMUP_ITERS=1
  MEAS_ITERS=2
  ITER_TIME=3s
  RECORDS=5000
  FORKS=1
  BACKENDS_KV='hashmap,rocksdb,sqlite,lucene,tantivy,rocksdb+tantivy'
  BACKENDS_COLL='sqlite,lucene,tantivy,rocksdb+tantivy'
  BATCH='buffered'
  THREADS_LIST=(1 4)
else
  WARMUP_ITERS=5
  MEAS_ITERS=5
  ITER_TIME=10s
  RECORDS=100000
  FORKS=2
  BACKENDS_KV='hashmap,mapdb,lmdb,rocksdb,halodb,chroniclemap,sqlite,h2,duckdb,lucene,tantivy,rocksdb+lucene,rocksdb+tantivy,lmdb+lucene,lmdb+tantivy,halodb+lucene,halodb+tantivy'
  BACKENDS_COLL='sqlite,h2,duckdb,lucene,tantivy,rocksdb+lucene,rocksdb+tantivy,lmdb+lucene,lmdb+tantivy,halodb+lucene,halodb+tantivy'
  BATCH='buffered,async'
  THREADS_LIST=(1 4 16)
fi

JMH_COMMON=(
  -wi "$WARMUP_ITERS"
  -i  "$MEAS_ITERS"
  -w  "$ITER_TIME"
  -r  "$ITER_TIME"
  -f  "$FORKS"
  -rf json
  -p  "recordCount=$RECORDS"
)

# --- build benchmark filter regexes ---------------------------------------------------------
#
# Each state class (KV vs Collection) takes a different `backend` value list, so we MUST run
# the suite in two passes — one targeting CompleteKvState benchmarks (Read/Write/Concurrency),
# one targeting CompleteCollectionState benchmarks (Query). JMH applies `-p backend=...` to
# every selected benchmark in a single invocation, so mixing them would feed Collection
# benchmarks an invalid backend (or vice versa).

CONCURRENCY_FILTERS=()
for t in "${THREADS_LIST[@]}"; do
  CONCURRENCY_FILTERS+=("benchmark.jmh.complete.CompleteConcurrency${t}Benchmark")
done

KV_INCLUDES=(
  'benchmark.jmh.complete.CompleteReadBenchmark'
  'benchmark.jmh.complete.CompleteWriteBenchmark'
  "${CONCURRENCY_FILTERS[@]}"
)
COLL_INCLUDES=('benchmark.jmh.complete.CompleteQueryBenchmark')

KV_REGEX="^($(IFS='|'; echo "${KV_INCLUDES[*]}"))\\."
COLL_REGEX="^($(IFS='|'; echo "${COLL_INCLUDES[*]}"))\\."

echo "[run-complete.sh] mode=$MODE records=$RECORDS warmup=$WARMUP_ITERS meas=$MEAS_ITERS iter=$ITER_TIME forks=$FORKS"
echo "[run-complete.sh] kv backends:    $BACKENDS_KV"
echo "[run-complete.sh] coll backends:  $BACKENDS_COLL"
echo "[run-complete.sh] threads:        ${THREADS_LIST[*]}"
echo "[run-complete.sh] kv  results →   $RESULT_KV"
echo "[run-complete.sh] coll results →  $RESULT_COLL"
echo "[run-complete.sh] log →           $LOG_FILE"
echo

# Pass 1: all-backends (KV state) — read, write, concurrency
KV_ARGS=(
  "$KV_REGEX"
  "${JMH_COMMON[@]}"
  -rff "$RESULT_KV"
  -p   "backend=$BACKENDS_KV"
  -p   "batch=$BATCH"
  "${EXTRA_ARGS[@]}"
)

# Pass 2: collection-only — query benchmarks
COLL_ARGS=(
  "$COLL_REGEX"
  "${JMH_COMMON[@]}"
  -rff "$RESULT_COLL"
  -p   "backend=$BACKENDS_COLL"
  "${EXTRA_ARGS[@]}"
)

{
  echo "===== pass 1/2: KV + write + concurrency ====="
  sbt -Dsbt.log.noformat=true \
      "benchmark/Jmh/run ${KV_ARGS[*]}" \
    </dev/null

  echo
  echo "===== pass 2/2: collection queries ====="
  sbt -Dsbt.log.noformat=true \
      "benchmark/Jmh/run ${COLL_ARGS[*]}" \
    </dev/null

  echo
  echo "===== render: markdown + SVG charts ====="
  REPORT_DIR="$REPO_ROOT/benchmark/target/report-${MODE}-${TS}"
  TITLE="LightDB Benchmarks — ${MODE} (${TS})"
  sbt -Dsbt.log.noformat=true \
      "benchmark/runMain benchmark.jmh.complete.RenderResults --out $REPORT_DIR --title \"$TITLE\" $RESULT_KV $RESULT_COLL" \
    </dev/null
} 2>&1 | tee "$LOG_FILE"

echo
echo "[run-complete.sh] done"
echo "[run-complete.sh]   kv  results:  $RESULT_KV"
echo "[run-complete.sh]   coll results: $RESULT_COLL"
echo "[run-complete.sh]   report:       $REPO_ROOT/benchmark/target/report-${MODE}-${TS}/report.md"
