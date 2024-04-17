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

## 1.0 TODO
- [ ] Full implementations for indexers
  - [ ] Apache Lucene index types
  - [ ] SQLite index types
- [ ] More performance improvements to SQLite integration
- [ ] Better RocksDB performance
- [ ] Automated generation of performance bar charts
- [ ] Create backup and restore features
    - [ ] Real-time backup (write changes to incremental file)
    - [ ] Complete dump and restore
    - [ ] Post-restore incremental restore
    - [ ] Testing of empty database loads from backups if available
- [ ] Data integrity checks
    - [ ] Verify data identical between store and index
    - [ ] Rebuild index from store