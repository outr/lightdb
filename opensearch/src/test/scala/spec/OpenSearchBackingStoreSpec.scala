package spec

import fabric.obj
import fabric.rw.{boolRW, longRW}
import lightdb.backup.DatabaseBackup
import lightdb.opensearch.{OpenSearchIndexName, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Persistence, StoredValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * Minimal check to ensure LightDB StoredValue backing store works on OpenSearch:
 * it should persist multiple StoredValues as distinct KeyValue docs.
 */
@EmbeddedTest
class OpenSearchBackingStoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport { spec =>
  object DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchBackingStoreSpec"
    override lazy val directory: Option[Path] = None

    val a: StoredValue[Boolean] = stored[Boolean]("a", default = false, persistence = Persistence.Stored)
    val b: StoredValue[Long] = stored[Long]("b", default = 0L, persistence = Persistence.Stored)

    object Upgrade extends DatabaseUpgrade {
      override def applyToNew: Boolean = true
      override def blockStartup: Boolean = true
      override def alwaysRun: Boolean = false
      override def upgrade(ldb: LightDB): Task[Unit] =
        a.set(true).and(b.set(123L)).unit
    }

    override def upgrades: List[DatabaseUpgrade] = List(Upgrade)
  }

  "OpenSearch backingStore" should {
    "persist multiple StoredValues as distinct KeyValue docs" in {
      val test = for {
        _ <- DB.init
        c <- DB.backingStore.t.count
        ids <- DB.backingStore.transaction { tx =>
          tx.query.id.stream.toList
        }
        _ <- Task(scribe.info(s"backingStore count=$c ids=${ids.map(_.value).sorted.mkString(", ")}"))
        // Expect at least: _databaseInitialized, _appliedUpgrades, a, b
        _ <- Task(c should be >= 4)
        _ <- DB.dispose
      } yield succeed
      test
    }
  }
}


