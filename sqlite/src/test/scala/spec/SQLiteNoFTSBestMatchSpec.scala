package spec

import lightdb.sql.SQLiteStore
import org.scalatest.BeforeAndAfterAll

/** Best-match ranking and whole-word matching on SQLite with FTS5 turned off: the shared ranking over `lightdb_words`. */
@EmbeddedTest
class SQLiteNoFTSBestMatchSpec extends AbstractBestMatchSpec with BeforeAndAfterAll {
  override def storeManager: SQLiteStore.type = SQLiteStore

  override protected def beforeAll(): Unit = {
    SQLiteStore.EnableFTS = false
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally SQLiteStore.EnableFTS = true
  }
}
