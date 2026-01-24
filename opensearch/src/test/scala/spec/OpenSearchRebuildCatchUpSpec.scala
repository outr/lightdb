package spec

import fabric._
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildCatchUpSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  "OpenSearchRebuild (two-phase)" should {
    "switch write alias first, then swap read alias after catch-up" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildCatchUpSpec"
      }, collectionName = "rebuild_catchup_spec")

      val client = OpenSearchClient(config)

      val readAlias = "rebuild_catchup_read"
      val writeAlias = "rebuild_catchup_write"

      val oldPhysical = s"${readAlias}_000001"
      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val initialDocs = rapid.Stream.emits(List(
        OpenSearchRebuild.RebuildDoc("d1", obj("v" -> str("one"))),
        OpenSearchRebuild.RebuildDoc("d2", obj("v" -> str("two")))
      ))

      val test = for
        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        _ <- client.deleteIndex(oldPhysical)

        // establish baseline: old index + read+write aliases -> old
        _ <- client.createIndex(oldPhysical, indexBody)
        _ <- client.updateAliases(obj("actions" -> arr(
          obj("add" -> obj("index" -> str(oldPhysical), "alias" -> str(readAlias))),
          obj("add" -> obj("index" -> str(oldPhysical), "alias" -> str(writeAlias), "is_write_index" -> bool(true)))
        )))
        _ <- client.indexDoc(writeAlias, "d1", obj("v" -> str("one")), refresh = Some("true"))
        _ <- client.indexDoc(writeAlias, "d2", obj("v" -> str("two")), refresh = Some("true"))
        before <- matchAllCount(readAlias)

        // run two-phase rebuild with empty catch-up (we'll simulate an in-flight write after switching write alias)
        newPhysical <- OpenSearchRebuild.rebuildSwitchWriteCatchUpSwapRead(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          initialDocs = initialDocs,
          catchUpDocs = Task.pure(rapid.Stream.empty),
          config = config,
          defaultSuffix = "_000001",
          refreshAfterInitial = true,
          refreshAfterCatchUp = true
        )

        // after swap, both reads and writes should go to new; simulate one more write and observe it via read alias
        _ <- client.indexDoc(writeAlias, "d3", obj("v" -> str("three")), refresh = Some("true"))
        after <- matchAllCount(readAlias)

        // cleanup
        _ <- client.deleteIndex(oldPhysical)
        _ <- client.deleteIndex(newPhysical)
      yield {
        before shouldBe 2
        after shouldBe 3
        newPhysical should not be oldPhysical
      }

      test
    }
  }
}


