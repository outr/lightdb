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
  - [ ] HikariCP for connection pooling
  - [ ] Property transactions without autocommit
- [ ] Automated generation of performance bar charts
- [ ] Rewrite
  - [X] Cross-Platform
  - [X] Transactions
  - [X] Single listener to capture all events on a Collection
  - [X] No AbstractCollection
  - [X] DB.collection and / Model separation
  - [ ] No IndexSupport, simply instantiated `index` and pass ref to collection
    - [ ] Support for multiple indexes on the same collection
    - [ ] Indexes have no implementation logic
  - [ ] Update benchmark to provide better async processing and JMH
  - [X] Generic unit tests with multiple implementations
  - [ ] Add btree supported indexes in Store
  - [ ] Add Redis support
  - [ ] Support SQL as a Store as well as Indexer
  - [ ] Support H2 as another SQL database
  - [ ] Provide a Scala.js implementation of Store using Web Storage API
  - [ ] Provide a Scala.js implementation using IndexedDB