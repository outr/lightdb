#!/bin/bash

declare -a arr=("ldbHaloLucene" "ldbMapLucene" "ldbRocksLucene" "ldbAtomicLucene" "ldbMapLucene" "ldbHaloSQLite" "ldbHaloH2" "ldbHaloDuck")

for i in "${arr[@]}"
do
  sbt "benchmark / runMain benchmark.bench.Runner $i"
done