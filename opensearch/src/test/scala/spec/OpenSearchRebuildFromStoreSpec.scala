package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.hashmap.HashMapStore
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRebuildFromStoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]
  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  object SourceDB extends LightDB {
    override type SM = HashMapStore.type
    override val storeManager: HashMapStore.type = HashMapStore
    override def directory = None
    override def upgrades = Nil
    lazy val docs = store[Doc, Doc.type](Doc, name = Some("source_docs"))
  }

  "OpenSearchRebuild (from store)" should {
    "rebuild an index by streaming docs from a source store transaction" in {
      val cfg = OpenSearchConfig.from(new lightdb.LightDB {
        override type SM = lightdb.store.CollectionManager
        override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
        override def directory = None
        override def upgrades = Nil
        override def name: String = "OpenSearchRebuildFromStoreSpec"
      }, collectionName = "rebuild_from_store")

      val client = OpenSearchClient(cfg)

      val readAlias = "rebuild_from_store_read"
      val writeAlias = Some("rebuild_from_store_write")
      val indexBody = lightdb.opensearch.OpenSearchTemplates.indexBody(
        model = Doc,
        fields = Doc.fields,
        config = cfg,
        storeName = "Doc",
        maxResultWindow = cfg.maxResultWindow
      )

      val test = for
        _ <- SourceDB.init
        _ <- SourceDB.docs.t.truncate
        _ <- SourceDB.docs.t.insert(List(Doc("x"), Doc("y"), Doc("z")))

        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- client.aliasTargets(writeAlias.get)
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))

        newPhysical <- OpenSearchRebuild.rebuildAndRepointAliasesFromStore(
          client = client,
          readAlias = readAlias,
          writeAlias = writeAlias,
          indexBody = indexBody,
          storeName = "Doc",
          fields = Doc.fields,
          source = SourceDB.docs,
          config = cfg,
          defaultSuffix = "_000001",
          refreshAfter = true
        )

        count <- client.count(readAlias, fabric.obj("query" -> fabric.obj("match_all" -> fabric.obj())))
        _ <- client.deleteIndex(newPhysical)
      yield {
        count shouldBe 3
      }

      test
    }
  }
}


