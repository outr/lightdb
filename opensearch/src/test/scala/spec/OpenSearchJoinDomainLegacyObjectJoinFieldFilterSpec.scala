package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchIndexName, OpenSearchTemplates}
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * Regression repro for a join-domain migration edge case:
 *
 * If the join field (default `__lightdb_join`) is NOT mapped as OpenSearch `"type": "join"` (e.g. legacy indices that
 * dynamically mapped it as an object `{ "name": "...", "parent": "..." }`), LightDB's join-type scoping filter
 * must still return documents for the logical store.
 *
 * Current behavior (bug): OpenSearchTransaction filters join-domain queries via `term(joinFieldName, store.name)`,
 * which matches 0 docs when the join field is an object.
 */
@EmbeddedTest
class OpenSearchJoinDomainLegacyObjectJoinFieldFilterSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with OpenSearchTestSupport {

  private val ChildStoreName = "LegacyChild"
  private val JoinDomain = "legacy_join_domain"
  private val JoinFieldName = "__legacy_join"

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
    override def name: String = "OpenSearchJoinDomainLegacyObjectJoinFieldFilterSpec"
    override lazy val directory: Option[Path] = Some(Path.of("db/OpenSearchJoinDomainLegacyObjectJoinFieldFilterSpec"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val children: lightdb.store.Collection[Child, Child.type] =
      store[Child, Child.type](Child, name = Some(ChildStoreName))
  }

  case class Child(parentId: String,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]

  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: F[String] = field("parentId", _.parentId)
  }

  "OpenSearch join-domain filtering" should {
    "return docs for a join-child store when the join field is legacy-mapped as an object (name/parent)" in {
      val kJoinField = "lightdb.opensearch.joinFieldName"
      val kDomain = s"lightdb.opensearch.$ChildStoreName.joinDomain"
      val kRole = s"lightdb.opensearch.$ChildStoreName.joinRole"
      val kParentField = s"lightdb.opensearch.$ChildStoreName.joinParentField"

      val prev = Map(
        kJoinField -> Profig(kJoinField).opt[String],
        kDomain -> Profig(kDomain).opt[String],
        kRole -> Profig(kRole).opt[String],
        kParentField -> Profig(kParentField).opt[String]
      )

      Profig(kJoinField).store(JoinFieldName)
      Profig(kDomain).store(JoinDomain)
      Profig(kRole).store("child")
      Profig(kParentField).store("parentId")

      val config = OpenSearchConfig.from(DB, collectionName = ChildStoreName)
      val client = OpenSearchClient(config)
      val indexName = OpenSearchIndexName.default(DB.name, ChildStoreName, config)

      val childDocId = "c1"
      val childParentId = "p1"
      val childSource = obj(
        // Not required for counting, but mirrors LightDB conventions.
        OpenSearchTemplates.InternalIdField -> str(childDocId),
        "parentId" -> str(childParentId),
        // Legacy join object mapping (dynamic) instead of `"type":"join"`.
        JoinFieldName -> obj("name" -> str(ChildStoreName), "parent" -> str(childParentId))
      )

      def restoreKeys(): Unit =
        prev.foreach {
          case (k, Some(v)) => Profig(k).store(v)
          case (k, None) => Profig(k).remove()
        }

      val test = for
        // Ensure the index exists with a "legacy" mapping (dynamic object join field).
        _ <- client.deleteIndex(indexName)
        _ <- client.createIndex(indexName, obj("mappings" -> obj("dynamic" -> bool(true))))
        _ <- client.indexDoc(indexName, childDocId, childSource, refresh = Some("true"))

        // Ground truth: OpenSearch can count docs by `__legacy_join.name.keyword`.
        rawCount <- client.count(
          indexName,
          obj("query" -> obj("term" -> obj(s"$JoinFieldName.name.keyword" -> str(ChildStoreName))))
        )

        // LightDB join scoping should match the raw count for this join type.
        _ <- DB.init
        lightdbCount <- DB.children.transaction(_.count)

        _ <- DB.dispose
        _ <- client.deleteIndex(indexName)
      yield {
        try {
          rawCount shouldBe 1
          // This is the regression assertion. It FAILS today (LightDB returns 0) until the join scoping filter is fixed.
          lightdbCount shouldBe rawCount
        } finally {
          restoreKeys()
        }
      }

      test
    }
  }
}

