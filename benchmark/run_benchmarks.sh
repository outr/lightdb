#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_FILE="${ROOT_DIR}/benchmark/benchmarks.json"
EXTRA_ARGS="${*:-}"

rm -f "${OUTPUT_FILE}"

sbt "benchmark/jmh:run -rf json -rff ${OUTPUT_FILE} ${EXTRA_ARGS}"

echo "JMH results written to ${OUTPUT_FILE}"
