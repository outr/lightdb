package spec

import fabric.*
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildSubsetSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  "OpenSearchRebuild (subset)" should {
    "delete a subset by query and reindex replacement docs" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildSubsetSpec"
      }, collectionName = "rebuild_subset_spec")

      val client = OpenSearchClient(config)

      val index = "rebuild_subset_spec_idx"
      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      val test = for
        _ <- client.deleteIndex(index)
        _ <- client.createIndex(index, indexBody)

        _ <- client.indexDoc(index, "d1", obj("v" -> str("one")), refresh = Some("true"))
        _ <- client.indexDoc(index, "d2", obj("v" -> str("two")), refresh = Some("true"))
        _ <- client.indexDoc(index, "d3", obj("v" -> str("three")), refresh = Some("true"))

        beforeD3 <- client.getDoc(index, "d3")

        // delete d2 + d3, reindex d2 (updated) and add d4
        deleteQ = obj("ids" -> obj("values" -> arr(str("d2"), str("d3"))))
        docs = rapid.Stream.emits(List(
          OpenSearchRebuild.RebuildDoc("d2", obj("v" -> str("two_updated"))),
          OpenSearchRebuild.RebuildDoc("d4", obj("v" -> str("four")))
        ))
        deleted <- OpenSearchRebuild.rebuildSubsetInIndex(
          client = client,
          index = index,
          deleteQuery = deleteQ,
          docs = docs,
          config = config,
          refreshAfter = true
        )

        afterD1 <- client.getDoc(index, "d1")
        afterD2 <- client.getDoc(index, "d2")
        afterD3 <- client.getDoc(index, "d3")
        afterD4 <- client.getDoc(index, "d4")
        count <- client.count(index, obj("query" -> obj("match_all" -> obj())))

        _ <- client.deleteIndex(index)
      yield {
        beforeD3.isDefined shouldBe true
        deleted shouldBe 2
        count shouldBe 3

        afterD1.flatMap(_.asObj.get("v")).map(_.asString) shouldBe Some("one")
        afterD2.flatMap(_.asObj.get("v")).map(_.asString) shouldBe Some("two_updated")
        afterD3 shouldBe None
        afterD4.flatMap(_.asObj.get("v")).map(_.asString) shouldBe Some("four")
      }

      test
    }
  }
}


