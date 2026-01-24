package spec

import fabric._
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildCatchUpLoopSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  private class CatchUpImpl extends (Option[String] => Task[Option[OpenSearchRebuild.CatchUpBatch]]) {
    private var n = 0

    override def apply(token: Option[String]): Task[Option[OpenSearchRebuild.CatchUpBatch]] = Task {
      n += 1
      if n == 1 then {
        Some(OpenSearchRebuild.CatchUpBatch(
          docs = List(OpenSearchRebuild.RebuildDoc("d2", obj("v" -> str("two_updated")))),
          nextToken = Some("t1")
        ))
      } else if n == 2 then {
        Some(OpenSearchRebuild.CatchUpBatch(
          docs = List(OpenSearchRebuild.RebuildDoc("d3", obj("v" -> str("three")))),
          nextToken = None
        ))
      } else {
        None
      }
    }
  }

  "OpenSearchRebuild (catch-up loop)" should {
    "apply multiple catch-up batches before swapping read alias" in {
      val config = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildCatchUpLoopSpec"
      }, collectionName = "rebuild_catchup_loop_spec")

      val client = OpenSearchClient(config)

      val readAlias = "rebuild_catchup_loop_read"
      val writeAlias = "rebuild_catchup_loop_write"

      val oldPhysical = s"${readAlias}_000001"
      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val initialDocs = rapid.Stream.emits(List(
        OpenSearchRebuild.RebuildDoc("d1", obj("v" -> str("one"))),
        OpenSearchRebuild.RebuildDoc("d2", obj("v" -> str("two")))
      ))

      val catchUp = new CatchUpImpl

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

        newPhysical <- OpenSearchRebuild.rebuildSwitchWriteCatchUpLoopSwapRead(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          initialDocs = initialDocs,
          catchUp = catchUp,
          config = config,
          defaultSuffix = "_000001",
          refreshAfterInitial = true,
          refreshBetweenCatchUpBatches = true,
          refreshAfterCatchUp = true,
          maxCatchUpBatches = 10
        )

        after <- matchAllCount(readAlias)
        d2 <- client.getDoc(readAlias, "d2")
        d3 <- client.getDoc(readAlias, "d3")

        _ <- client.deleteIndex(oldPhysical)
        _ <- client.deleteIndex(newPhysical)
      yield {
        before shouldBe 2
        after shouldBe 3
        d2.flatMap(_.asObj.get("v")).map(_.asString) shouldBe Some("two_updated")
        d3.flatMap(_.asObj.get("v")).map(_.asString) shouldBe Some("three")
      }

      test
    }
  }
}


