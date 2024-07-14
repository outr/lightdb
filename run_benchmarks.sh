#!/bin/bash

set -e

#declare -a arr=("SQLite" "H2" "Derby" "LightDB-SQLite" "LightDB-Map-SQLite" "LightDB-HaloDB-SQLite" "LightDB-Lucene" "LightDB-HaloDB-Lucene" "LightDB-H2")
declare -a arr=("LightDB-SQLite")

for i in "${arr[@]}"
do
  sbt "benchmark / runMain benchmark.bench.Runner $i"
done

sbt "benchmark / runMain benchmark.bench.ReportGenerator"