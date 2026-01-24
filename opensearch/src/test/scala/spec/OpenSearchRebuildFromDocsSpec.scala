package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildFromDocsSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]
  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  "OpenSearchRebuild (from docs)" should {
    "rebuild an index from a Stream[Doc] using the shared encoding rules" in {
      val cfg = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildFromDocsSpec"
      }, collectionName = "rebuild_from_docs")

      val client = OpenSearchClient(cfg)

      val readAlias = "rebuild_from_docs_read"
      val writeAlias = Some("rebuild_from_docs_write")
      val indexBody = lightdb.opensearch.OpenSearchTemplates.indexBody(
        model = Doc,
        fields = Doc.fields,
        config = cfg,
        storeName = "Doc",
        maxResultWindow = cfg.maxResultWindow
      )

      val docs = List(Doc("a"), Doc("b"), Doc("c"))
      val stream = rapid.Stream.emits(docs)

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, fabric.obj("query" -> fabric.obj("match_all" -> fabric.obj())))

      val test = for
        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias.get)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))

        newPhysical <- OpenSearchRebuild.rebuildAndRepointAliasesFromDocs(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          storeName = "Doc",
          fields = Doc.fields,
          docs = stream,
          config = cfg,
          defaultSuffix = "_000001",
          refreshAfter = true
        )

        count <- matchAllCount(readAlias)
        _ <- client.deleteIndex(newPhysical)
      yield {
        count shouldBe 3
      }

      test
    }
  }
}


