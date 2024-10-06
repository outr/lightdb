# lightdb
[![CI](https://github.com/outr/lightdb/actions/workflows/ci.yml/badge.svg)](https://github.com/outr/lightdb/actions/workflows/ci.yml)

Computationally focused database using pluggable store + indexer

## Provided Stores
- Yahoo's HaloDB (https://github.com/yahoo/HaloDB) - Preferred for performance
- MapDB (https://mapdb.org)
- Facebook's RocksDB (https://rocksdb.org)
- Redis (https://redis.io)

## Provided Indexers
- Apache Lucene (https://lucene.apache.org) - Most featureful
- SQLite (https://www.sqlite.org) - Fastest
- H2 (https://h2database.com)
- DuckDB (https://duckdb.org) - Experimental

## 1.0 TODO
- [X] More performance improvements to SQLite integration
  - [X] HikariCP for connection pooling
  - [X] Property transactions without autocommit
- [ ] New benchmarks
  - [ ] JMH
  - [ ] Better parallel processing
  - [ ] Automated generation of performance bar charts
  - [ ] Shell script to run all benchmarks
- [ ] Rewrite
  - [X] Cross-Platform
  - [X] Transactions
  - [X] Single listener to capture all events on a Collection
  - [X] No AbstractCollection
  - [X] DB.collection and / Model separation
  - [X] Remove Pagination for better streaming "SearchResults"
  - [X] No IndexSupport, simply instantiated `index` and pass ref to collection
    - [X] Support for multiple indexes on the same collection
  - [X] Generic unit tests with multiple implementations
  - [ ] Document classes
  - [ ] MDoc support
- [X] New Integrations
  - [X] Add Redis support
  - [X] Support H2 as another SQL database
  
## 1.1 TODO
- [ ] Add btree supported indexes in Store
- [ ] Provide a Scala.js implementation of Store using Web Storage API
- [ ] Provide a Scala.js implementation using IndexedDB
- [ ] ScalaNative support