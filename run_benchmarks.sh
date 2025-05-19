#!/bin/bash

set -e

#declare -a arr=("SQLite" "H2" "Derby" "LightDB-SQLite" "LightDB-Map-SQLite" "LightDB-HaloDB-SQLite" "LightDB-Lucene" "LightDB-HaloDB-Lucene" "LightDB-H2" "LightDB-HaloDB-H2")
#declare -a arr=("LightDB-SQLite" "LightDB-Map-SQLite" "LightDB-HaloDB-SQLite" "LightDB-Lucene" "LightDB-HaloDB-Lucene" "LightDB-H2" "LightDB-HaloDB-H2")
#declare -a arr=("LightDB-Lucene" "LightDB-HaloDB-Lucene")
#declare -a arr=("SQLite" "PostgreSQL" "H2" "Derby" "MongoDB" "LightDB-SQLite" "LightDB-Map-SQLite" "LightDB-HaloDB-SQLite" "LightDB-Lucene" "LightDB-HaloDB-Lucene" "LightDB-RocksDB-Lucene" "LightDB-H2" "LightDB-HaloDB-H2")
declare -a arr=("SQLite")

for i in "${arr[@]}"
do
  sbt "benchmark / runMain benchmark.bench.Runner $i"
done

sbt "benchmark / runMain benchmark.bench.ReportGenerator"