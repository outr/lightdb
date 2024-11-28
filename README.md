# lightdb
[![CI](https://github.com/outr/lightdb/actions/workflows/ci.yml/badge.svg)](https://github.com/outr/lightdb/actions/workflows/ci.yml)

Computationally focused database using pluggable stores

## Provided Stores
- Yahoo's HaloDB (https://github.com/yahoo/HaloDB)
- MapDB (https://mapdb.org)
- Facebook's RocksDB (https://rocksdb.org)
- Redis (https://redis.io)
- Apache Lucene (https://lucene.apache.org)
- SQLite (https://www.sqlite.org)
- H2 (https://h2database.com)
- DuckDB (https://duckdb.org)
- PostgreSQL (https://www.postgresql.org)
- Redis (https://redis.io)

## SBT Configuration

To add all modules:
```scala
libraryDependencies += "com.outr" %% "lightdb-all" % "0.17.0"
```

For a specific implementation like Lucene:
```scala
libraryDependencies += "com.outr" %% "lightdb-lucene" % "0.17.0"
```