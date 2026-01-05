#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Opt-in IMDB performance suite runner for traversal backend.
#
# Override defaults via environment variables:
# - IMDB_PERF_RECORDS   (default: 500000)
# - IMDB_PERF_BATCH     (default: 5000)
# - IMDB_PERF_DATA_DIR  (default: data)
# - IMDB_PERF_HOP_LIMIT (default: 10000)
# - IMDB_PERF_HOP_DEPTH (default: 3)
#
# Example:
#   IMDB_PERF_RECORDS=200000 IMDB_PERF_DATA_DIR=/path/to/imdb \
#     ./scripts/run-traversal-imdb-perf.sh

IMDB_PERF_RECORDS="${IMDB_PERF_RECORDS:-500000}"
IMDB_PERF_BATCH="${IMDB_PERF_BATCH:-5000}"
IMDB_PERF_DATA_DIR="${IMDB_PERF_DATA_DIR:-data}"
IMDB_PERF_HOP_LIMIT="${IMDB_PERF_HOP_LIMIT:-10000}"
IMDB_PERF_HOP_DEPTH="${IMDB_PERF_HOP_DEPTH:-3}"

EXTRA_SBT_ARGS=()
for arg in "$@"; do
  # Convenience: allow passing overrides as CLI args like IMDB_PERF_RECORDS=2000
  if [[ "$arg" == IMDB_PERF_RECORDS=* ]]; then IMDB_PERF_RECORDS="${arg#*=}"; continue; fi
  if [[ "$arg" == IMDB_PERF_BATCH=* ]]; then IMDB_PERF_BATCH="${arg#*=}"; continue; fi
  if [[ "$arg" == IMDB_PERF_DATA_DIR=* ]]; then IMDB_PERF_DATA_DIR="${arg#*=}"; continue; fi
  if [[ "$arg" == IMDB_PERF_HOP_LIMIT=* ]]; then IMDB_PERF_HOP_LIMIT="${arg#*=}"; continue; fi
  if [[ "$arg" == IMDB_PERF_HOP_DEPTH=* ]]; then IMDB_PERF_HOP_DEPTH="${arg#*=}"; continue; fi
  EXTRA_SBT_ARGS+=("$arg")
done

# NOTE: tests are forked (see build.sbt), so we must pass -D props via Test/javaOptions
# rather than as JVM opts to sbt itself.
JAVA_PROPS=(
  "-Dimdb.perf=true"
  "-Dimdb.perf.records=${IMDB_PERF_RECORDS}"
  "-Dimdb.perf.batch=${IMDB_PERF_BATCH}"
  "-Dimdb.perf.dataDir=${IMDB_PERF_DATA_DIR}"
  "-Dimdb.perf.hopLimit=${IMDB_PERF_HOP_LIMIT}"
  "-Dimdb.perf.hopDepth=${IMDB_PERF_HOP_DEPTH}"
)

JAVA_PROPS_SCALA_LIST="$(printf "\"%s\"," "${JAVA_PROPS[@]}")"
JAVA_PROPS_SCALA_LIST="${JAVA_PROPS_SCALA_LIST%,}"
SET_JAVA_OPTS_CMD="set benchmark / Test / javaOptions ++= Seq(${JAVA_PROPS_SCALA_LIST})"

SBT_CMD=(
  sbt
  --no-colors
  "${SET_JAVA_OPTS_CMD}"
  "benchmark/testOnly spec.TraversalImdbPerformanceSpec"
)

echo "Running traversal IMDB perf spec with:"
echo "  IMDB_PERF_RECORDS=${IMDB_PERF_RECORDS}"
echo "  IMDB_PERF_BATCH=${IMDB_PERF_BATCH}"
echo "  IMDB_PERF_DATA_DIR=${IMDB_PERF_DATA_DIR}"
echo "  IMDB_PERF_HOP_LIMIT=${IMDB_PERF_HOP_LIMIT}"
echo "  IMDB_PERF_HOP_DEPTH=${IMDB_PERF_HOP_DEPTH}"

if [[ ${#EXTRA_SBT_ARGS[@]} -gt 0 ]]; then
  echo "  EXTRA_SBT_ARGS=${EXTRA_SBT_ARGS[*]}"
fi

exec "${SBT_CMD[@]}" "${EXTRA_SBT_ARGS[@]}"


