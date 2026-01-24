package spec

import fabric._
import lightdb.opensearch.OpenSearchIndexMigration
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchAliasSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  "OpenSearch aliases" should {
    "support swapping a read alias between indices" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchAliasSpec"
      }, collectionName = "alias_test")
      val client = OpenSearchClient(config)

      val indexA = "alias_spec_a"
      val indexB = "alias_spec_b"
      val alias = "alias_spec_read"

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val test = for
        _ <- client.deleteIndex(indexA)
        _ <- client.deleteIndex(indexB)
        _ <- client.createIndex(indexA, obj("mappings" -> obj("dynamic" -> bool(true))))
        _ <- client.createIndex(indexB, obj("mappings" -> obj("dynamic" -> bool(true))))
        _ <- client.indexDoc(indexA, "a1", obj("value" -> str("one")), refresh = Some("true"))
        _ <- client.indexDoc(indexB, "b1", obj("value" -> str("two")), refresh = Some("true"))
        _ <- OpenSearchIndexMigration.repointAlias(client, alias, indexA)
        c1 <- matchAllCount(alias)
        _ <- OpenSearchIndexMigration.repointAlias(client, alias, indexB)
        c2 <- matchAllCount(alias)
        _ <- client.deleteIndex(indexA)
        _ <- client.deleteIndex(indexB)
      yield {
        c1 should be(1)
        c2 should be(1)
      }

      test
    }

    "support creating a new physical index and repointing read/write aliases" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchAliasSpec2"
      }, collectionName = "alias_test_2")
      val client = OpenSearchClient(config)

      val readAlias = "alias_spec2_read"
      val writeAlias = Some("alias_spec2_write")

      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      val test = for
        // Clean up any existing aliases/indices from prior runs
        existing <- client.aliasTargets(readAlias)
        _ <- existing.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias.get)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))

        created <- OpenSearchIndexMigration.createIndexAndRepointAliases(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          defaultSuffix = "_000001"
        )
        // Write through write alias and read through read alias
        _ <- client.indexDoc(writeAlias.get, "x1", obj("value" -> str("one")), refresh = Some("true"))
        c <- client.count(readAlias, obj("query" -> obj("match_all" -> obj())))
        // cleanup
        _ <- client.deleteIndex(created)
      yield {
        c should be(1)
      }

      test
    }
  }
}


