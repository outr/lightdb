package spec

import lightdb.rocksdb.RocksDBStore

import java.nio.file.Path

@EmbeddedTest
class RocksDBFileStorageSpec extends AbstractFileStorageSpec(RocksDBStore, Some(Path.of("db/RocksDBFileStorageSpec")))