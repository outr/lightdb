package spec

import lightdb.mapdb.MapDBStore

import java.nio.file.Path

@EmbeddedTest
class MapDBFileStorageSpec
  extends AbstractFileStorageSpec(MapDBStore, Some(Path.of("db/MapDBFileStorageSpec")))