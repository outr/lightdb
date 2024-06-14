# lightdb
[![CI](https://github.com/outr/lightdb/actions/workflows/ci.yml/badge.svg)](https://github.com/outr/lightdb/actions/workflows/ci.yml)

Computationally focused database using pluggable store + indexer

## Provided Stores
- Yahoo's HaloDB (https://github.com/yahoo/HaloDB) - Preferred for performance
- MapDB (https://mapdb.org)
- Facebook's RocksDB (https://rocksdb.org)

## Provided Indexers
- Apache Lucene (https://lucene.apache.org) - Most featureful
- SQLite (https://www.sqlite.org) - Fastest
- DuckDB (https://duckdb.org) - Experimental

## 1.0 TODO
- [ ] More performance improvements to SQLite integration
- [ ] Automated generation of performance bar charts
- [ ] Rewrite
  - [ ] Cross-Platform
  - [ ] Transactions
  - [ ] Single listener to capture all events on a Collection
  - [ ] No AbstractCollection
  - [ ] DB.collection and / Model separation
  - [ ] No IndexSupport, simply instantiated `index` and pass ref to collection
  - [ ] Update benchmark to provide better async processing and JMH
  - [ ] Generic unit tests with multiple implementations