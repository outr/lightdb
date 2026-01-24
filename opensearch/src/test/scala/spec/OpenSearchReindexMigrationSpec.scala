package spec

import fabric.*
import lightdb.opensearch.OpenSearchIndexMigration
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchReindexMigrationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  "OpenSearch reindex migrations" should {
    "reindex from a read alias into a new physical index and swap aliases" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchReindexMigrationSpec"
      }, collectionName = "reindex_migration_test")
      val client = OpenSearchClient(config)

      val readAlias = "reindex_spec_read"
      val writeAlias = Some("reindex_spec_write")

      val oldPhysical = "reindex_spec_read_000001"
      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val test = for
        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias.get)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        _ <- client.deleteIndex(oldPhysical)

        // create initial index + aliases and index a couple docs
        _ <- client.createIndex(oldPhysical, indexBody)
        _ <- OpenSearchIndexMigration.repointReadWriteAliases(client, readAlias, writeAlias, oldPhysical)
        _ <- client.indexDoc(writeAlias.get, "d1", obj("v" -> str("one")), refresh = Some("true"))
        _ <- client.indexDoc(writeAlias.get, "d2", obj("v" -> str("two")), refresh = Some("true"))
        before <- matchAllCount(readAlias)

        // migrate to new physical index via reindex + alias swap
        newPhysical <- OpenSearchIndexMigration.reindexAndRepointAliases(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          newIndexBody = indexBody,
          defaultSuffix = "_000001"
        )
        after <- matchAllCount(readAlias)

        // cleanup
        _ <- client.deleteIndex(oldPhysical)
        _ <- client.deleteIndex(newPhysical)
      yield {
        before should be(2)
        after should be(2)
      }

      test
    }
  }
}


