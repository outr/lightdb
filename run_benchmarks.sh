#!/bin/bash

set -e

#declare -a arr=("ldbHaloLucene" "ldbMapLucene" "ldbRocksLucene" "ldbAtomicLucene" "ldbMapLucene" "ldbHaloSQLite" "ldbHaloH2" "ldbHaloDuck")
#declare -a arr=("ldbHaloLucene" "SQLite")
#declare -a arr=("PostgreSQL")
declare -a arr=("LightDB-Lucene")

for i in "${arr[@]}"
do
  sbt "benchmark / runMain benchmark.bench.Runner $i"
done

sbt "benchmark / runMain benchmark.bench.ReportGenerator"