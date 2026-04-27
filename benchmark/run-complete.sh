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
# Output (one directory per run, named `benchmark/results/<lightdb-version>-<timestamp>/`):
#   benchmark/results/<version>-<timestamp>/<mode>.log              # combined console log
#   benchmark/results/<version>-<timestamp>/jmh-results-kv.json     # all-backends pass
#   benchmark/results/<version>-<timestamp>/jmh-results-coll.json   # collection-only pass
#   benchmark/results/<version>-<timestamp>/report.md               # rendered Markdown
#   benchmark/results/<version>-<timestamp>/*.svg                   # one chart per benchmark
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

# Extract the project version from build.sbt's `ThisBuild / version := "x.y.z"` line so the
# output directory makes it obvious which build of LightDB the numbers came from.
VERSION=$(grep -E '^ThisBuild / version :=' build.sbt | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
if [[ -z "$VERSION" ]]; then
  echo "could not parse LightDB version from build.sbt" >&2; exit 2
fi

TS="$(date +%Y%m%d-%H%M%S)"
RESULT_DIR="$REPO_ROOT/benchmark/results/${VERSION}-${TS}"
mkdir -p "$RESULT_DIR"

# JMH resolves the result file relative to its forked process cwd, which sbt sets unpredictably
# — always pass an absolute path so the file lands where we expect.
RESULT_KV="$RESULT_DIR/jmh-results-kv.json"
RESULT_COLL="$RESULT_DIR/jmh-results-coll.json"
LOG_FILE="$RESULT_DIR/${MODE}.log"

# --- mode-specific knobs --------------------------------------------------------------------

#
# Note: `hashmap` is intentionally excluded from both modes' default backend lists. As a pure
# in-memory store its ops are 50–500× faster than any persistent backend, which compresses every
# real backend into a single near-zero-pixel bar on the rendered SVG charts. Pass `-p
# backend=hashmap` explicitly if you want to include it.
#
# `fast` and `full` cover the SAME backends — fast just dials down recordCount, iter count, fork
# count, and concurrency levels so the suite finishes in ~30 minutes instead of overnight while
# still touching every backend at least once for shape validation + smoke-test the SVG output.
BACKENDS_KV='mapdb,lmdb,rocksdb,halodb,chroniclemap,sqlite,h2,duckdb,lucene,tantivy,rocksdb+lucene,rocksdb+tantivy,lmdb+lucene,lmdb+tantivy,halodb+lucene,halodb+tantivy'
BACKENDS_COLL='sqlite,h2,duckdb,lucene,tantivy,rocksdb+lucene,rocksdb+tantivy,lmdb+lucene,lmdb+tantivy,halodb+lucene,halodb+tantivy'

if [[ "$MODE" == "fast" ]]; then
  WARMUP_ITERS=1
  MEAS_ITERS=2
  ITER_TIME=3s
  RECORDS=5000
  FORKS=1
  BATCH='buffered'
  THREADS_LIST=(1 4)
else
  WARMUP_ITERS=5
  MEAS_ITERS=5
  ITER_TIME=10s
  RECORDS=100000
  FORKS=2
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

echo "[run-complete.sh] lightdb $VERSION · mode=$MODE records=$RECORDS warmup=$WARMUP_ITERS meas=$MEAS_ITERS iter=$ITER_TIME forks=$FORKS"
echo "[run-complete.sh] kv backends:    $BACKENDS_KV"
echo "[run-complete.sh] coll backends:  $BACKENDS_COLL"
echo "[run-complete.sh] threads:        ${THREADS_LIST[*]}"
echo "[run-complete.sh] output dir:     $RESULT_DIR"
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
  TITLE="LightDB ${VERSION} — ${MODE} (${TS})"
  sbt -Dsbt.log.noformat=true \
      "benchmark/runMain benchmark.jmh.complete.RenderResults --out $RESULT_DIR --title \"$TITLE\" $RESULT_KV $RESULT_COLL" \
    </dev/null
} 2>&1 | tee "$LOG_FILE"

echo
echo "[run-complete.sh] done"
echo "[run-complete.sh]   results dir:  $RESULT_DIR"
echo "[run-complete.sh]   report:       $RESULT_DIR/report.md"
