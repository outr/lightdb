package spec

import fabric.*
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.opensearch.{OpenSearchIndexName, OpenSearchStore, OpenSearchTemplates}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchMappingHashAutoMigrateSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class AutoMigrateDoc(name: String, _id: Id[AutoMigrateDoc] = AutoMigrateDoc.id()) extends Document[AutoMigrateDoc]
  object AutoMigrateDoc extends DocumentModel[AutoMigrateDoc] with JsonConversion[AutoMigrateDoc] {
    override implicit val rw: RW[AutoMigrateDoc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[AutoMigrateDoc, AutoMigrateDoc.type] = store(AutoMigrateDoc)
  }

  "OpenSearch mapping hash auto-migrate" should {
    "auto-reindex+swap aliases when mapping hash mismatches and mappingHashAutoMigrate=true" in {
      val db = new DB
      val storeName = AutoMigrateDoc.getClass.getSimpleName.stripSuffix("$") // store name is derived from model in LightDB; we use db.docs.name after init

      // We need storeName + config before init; use db.docs directly.
      val s = db.docs

      // Configure alias-backed indexing + auto-migration for this store.
      Profig(s"lightdb.opensearch.${s.name}.useIndexAlias").store(true)
      Profig(s"lightdb.opensearch.${s.name}.useWriteAlias").store(true)
      Profig(s"lightdb.opensearch.${s.name}.mappingHash.autoMigrate").store(true)
      Profig(s"lightdb.opensearch.${s.name}.mappingHash.warnOnly").store(true)

      val cfg = OpenSearchConfig.from(db, s.name)
      val client = OpenSearchClient(cfg)

      val readAlias = OpenSearchIndexName.default(db.name, s.name, cfg)
      val writeAlias = s"$readAlias${cfg.writeAliasSuffix}"
      val physical1 = s"$readAlias${cfg.indexAliasSuffix}"

      // Create a physical index with a WRONG mapping_hash, then point read+write aliases at it.
      val wrongBody = obj(
        "settings" -> obj("index" -> obj("max_result_window" -> num(cfg.maxResultWindow))),
        "mappings" -> obj(
          "dynamic" -> bool(true),
          "_meta" -> obj(
            "lightdb" -> obj(
              "mapping_hash" -> str("deadbeef"),
              "store" -> str(s.name)
            )
          ),
          "properties" -> obj(
            OpenSearchTemplates.InternalIdField -> obj("type" -> str("keyword"))
          )
        )
      )

      val docsToIndex = List(
        AutoMigrateDoc("a", Id("a")),
        AutoMigrateDoc("b", Id("b"))
      )

      val test = for
        _ <- client.deleteIndex(physical1).attempt.unit
        _ <- client.updateAliases(obj("actions" -> arr(
          obj("remove" -> obj("index" -> str(physical1), "alias" -> str(readAlias))),
          obj("remove" -> obj("index" -> str(physical1), "alias" -> str(writeAlias)))
        ))).attempt.unit
        _ <- client.createIndex(physical1, wrongBody)
        _ <- client.updateAliases(obj("actions" -> arr(
          obj("add" -> obj("index" -> str(physical1), "alias" -> str(readAlias))),
          obj("add" -> obj("index" -> str(physical1), "alias" -> str(writeAlias), "is_write_index" -> bool(true)))
        )))
        _ <- docsToIndex.foldLeft(Task.unit) { (acc, d) =>
          // Index through the alias so migration copies them.
          acc.next(client.indexDoc(readAlias, d._id.value, obj("name" -> str(d.name)), refresh = Some("true")))
        }
        _ <- db.init // should trigger auto-migrate and stop warning spam
        targets <- client.aliasTargets(readAlias)
        target = targets.headOption.getOrElse(throw new RuntimeException("aliasTargets empty after init"))
        actualHash <- client.mappingHash(target).map(_.getOrElse(""))
        expectedHash = OpenSearchTemplates.indexBody(AutoMigrateDoc, s.fields, cfg, s.name, maxResultWindow = cfg.maxResultWindow)
          .asObj.get("mappings").flatMap(_.asObj.get("_meta")).flatMap(_.asObj.get("lightdb")).flatMap(_.asObj.get("mapping_hash")).map(_.asString).getOrElse("")
        count <- client.count(readAlias, obj("query" -> obj("match_all" -> obj())))
        _ <- db.dispose
      yield {
        targets.length shouldBe 1
        target should not be physical1
        actualHash shouldBe expectedHash
        count shouldBe docsToIndex.length
      }

      test
    }
  }
}

