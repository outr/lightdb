package spec

import lightdb.lmdb.LMDBStore

import java.nio.file.Path

@EmbeddedTest
class LMDBFileStorageSpec
  extends AbstractFileStorageSpec(LMDBStore, Some(Path.of("db/LMDBFileStorageSpec")))

