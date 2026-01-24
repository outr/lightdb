package spec

import fabric.*
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  "OpenSearchRebuild" should {
    "rebuild a new physical index from a source stream and swap aliases" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildSpec"
      }, collectionName = "rebuild_spec")

      val client = OpenSearchClient(config)

      val readAlias = "rebuild_spec_read"
      val writeAlias = Some("rebuild_spec_write")

      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))
      val docs = rapid.Stream.emits(List(
        OpenSearchRebuild.RebuildDoc("d1", obj("v" -> str("one"))),
        OpenSearchRebuild.RebuildDoc("d2", obj("v" -> str("two"))),
        OpenSearchRebuild.RebuildDoc("d3", obj("v" -> str("three")))
      ))

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val test = for
        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias.get)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))

        // rebuild
        newPhysical <- OpenSearchRebuild.rebuildAndRepointAliases(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          docs = docs,
          config = config,
          defaultSuffix = "_000001",
          refreshAfter = true
        )

        count <- matchAllCount(readAlias)

        // cleanup
        _ <- client.deleteIndex(newPhysical)
      yield {
        count shouldBe 3
      }

      test
    }
  }
}


