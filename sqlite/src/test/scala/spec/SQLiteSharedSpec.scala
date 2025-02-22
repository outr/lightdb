package spec

import lightdb.sql.{SQLiteSharedStore, SQLiteStore}
import rapid.Task

import java.nio.file.Path

@EmbeddedTest
class SQLiteSharedSpec extends AbstractBasicSpec {
  override lazy val storeManager: SQLiteSharedStore = SQLiteSharedStore(SQLiteStore.singleConnectionManager(Some(Path.of("db/SQLiteSharedSpec"))))
}