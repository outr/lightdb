package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.Sort
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel, JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.query.{HighlightSpec, Highlights, InnerHit, InnerHitsSpec, DocWithInnerHits => InnerHitsResult}
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchInnerHitsSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with OpenSearchTestSupport
    with Matchers {

  // -- Fixtures ----------------------------------------------------------------------------
  // Mirrors the LogicalNetwork-style use case from the feature request: an outer "Entity"
  // matched via its child "Alias" records; the inner_hits should surface which alias caused
  // the entity to match.

  case class Entity(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Entity] = Entity.id()) extends RecordDocument[Entity]

  object Entity extends RecordDocumentModel[Entity]
      with JsonConversion[Entity]
      with ParentChildSupport[Entity, Alias, Alias.type] {
    override implicit val rw: RW[Entity] = RW.gen
    override def childStore: Collection[Alias, Alias.type] = DB.aliases
    override def parentField(childModel: Alias.type): Field[Alias, Id[Entity]] = childModel.entityId
    // Tokenized so the outer-hit highlight test gets a match query (highlighter doesn't
    // generate fragments for term queries on keyword fields).
    val name: T = field.tokenized(_.name)
  }

  case class Alias(entityId: Id[Entity],
                   alias: String,
                   priority: Int = 0,
                   description: String = "",
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Alias] = Alias.id()) extends RecordDocument[Alias]

  object Alias extends RecordDocumentModel[Alias] with JsonConversion[Alias] {
    override implicit val rw: RW[Alias] = RW.gen
    val entityId: I[Id[Entity]] = field.index(_.entityId)
    val alias: T = field.tokenized(_.alias)
    val priority: I[Int] = field.index(_.priority)
    val description: T = field.tokenized(_.description)
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = OpenSearchStore
    override def name: String = "OpenSearchInnerHitsSpec"
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$name"))
    val entities: Collection[Entity, Entity.type] = store(Entity)()
    val aliases: Collection[Alias, Alias.type] = store(Alias)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  // Three entities, multiple aliases each — one entity ("Devon") has the matching "EOG" alias,
  // mirroring the feature-request example.
  private val devon = Entity(name = "Devon")
  private val chevron = Entity(name = "Chevron")
  private val pioneer = Entity(name = "Pioneer Natural Resources")

  private val aliasData = List(
    Alias(entityId = devon._id, alias = "Devon Energy", priority = 100, description = "primary legal name"),
    Alias(entityId = devon._id, alias = "EOG Resources Inc", priority = 90, description = "predecessor entity name"),
    Alias(entityId = devon._id, alias = "Devon", priority = 80, description = "shortened reference"),
    Alias(entityId = chevron._id, alias = "Chevron Corporation", priority = 100, description = "primary legal name"),
    Alias(entityId = chevron._id, alias = "Standard Oil of California", priority = 50, description = "historical name"),
    Alias(entityId = pioneer._id, alias = "Pioneer Natural Resources", priority = 100, description = "primary legal name")
  )

  "OpenSearchInnerHitsSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }

    "index entities and aliases" in {
      DB.entities.transaction(_.upsert(List(devon, chevron, pioneer)))
        .next(DB.aliases.transaction(_.upsert(aliasData)))
        .unit
        .succeed
    }

    "return one matching alias per parent via inner_hits (default size)" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.alias === "EOG Resources Inc"))
          .docWithInnerHits
          .withInnerHits(DB.aliases, InnerHitsSpec(size = 1))
          .toList
          .map { rows =>
            rows.size should be(1)
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            row.doc.name should be("Devon")
            val matched = row.innerHitsFor[Alias](DB.aliases.name)
            matched.size should be(1)
            matched.head.doc.alias should be("EOG Resources Inc")
            matched.head.doc.entityId should be(devon._id)
          }
      }
    }

    "return multiple inner hits ordered by spec sort (size > 1)" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.entityId === devon._id))
          .docWithInnerHits
          .withInnerHits(DB.aliases, InnerHitsSpec(
            size = 5,
            sort = List(Sort.ByField(Alias.priority, SortDirection.Descending))
          ))
          .toList
          .map { rows =>
            rows.size should be(1)
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            val aliases = row.innerHitsFor[Alias](DB.aliases.name).map(_.doc.alias)
            // Descending by priority: 100, 90, 80
            aliases should be(List("Devon Energy", "EOG Resources Inc", "Devon"))
          }
      }
    }

    "_source filtering excludes unselected fields from inner hits" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.alias === "Devon Energy"))
          .docWithInnerHits
          .withInnerHits(DB.aliases, InnerHitsSpec(
            size = 1,
            sourceIncludes = List("alias", "entityId")  // skip `description` and `priority`
          ))
          .toList
          .map { rows =>
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            val hit = row.innerHitsFor[Alias](DB.aliases.name).head
            // Description omitted from `_source` → decoded as the empty default.
            hit.doc.alias should be("Devon Energy")
            hit.doc.description should be("")
          }
      }
    }

    "round-trip RW decoding produces a typed child Doc" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.alias === "Chevron Corporation"))
          .docWithInnerHits
          .withInnerHits(DB.aliases, InnerHitsSpec(size = 1))
          .toList
          .map { rows =>
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            val hit: InnerHit[Alias] = row.innerHitsFor[Alias](DB.aliases.name).head
            // Concrete typed access — `priority` is a typed Int, not a Json value.
            hit.doc.priority should be(100)
            hit.doc.alias should be("Chevron Corporation")
          }
      }
    }

    "highlight on inner_hits surfaces matched fragments" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.alias === "Devon"))
          .docWithInnerHits
          .withInnerHits(DB.aliases, InnerHitsSpec(
            size = 5,
            highlight = Some(HighlightSpec(fields = List("alias")))
          ))
          .toList
          .map { rows =>
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            val hits = row.innerHitsFor[Alias](DB.aliases.name)
            // At least one inner hit should carry a highlight fragment for `alias`.
            val anyHighlighted = hits.exists(_.highlights.nonEmpty)
            anyHighlighted should be(true)
            val frags = hits.flatMap(_.highlights.get("alias"))
            frags.exists(_.contains("<em>")) should be(true)
          }
      }
    }

    "outer-hit highlights populate Highlights on the result row" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.name === "Devon")
          .docWithInnerHits
          .withHighlight(HighlightSpec(fields = List("name")))
          .toList
          .map { rows =>
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            row.highlights.get("name").exists(_.contains("<em>")) should be(true)
          }
      }
    }

    "existing has_child query is unaffected when no inner-hits spec is set" in {
      DB.entities.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.alias === "EOG Resources Inc"))
          .docWithInnerHits
          .toList
          .map { rows =>
            rows.size should be(1)
            val row = rows.head.asInstanceOf[InnerHitsResult[Entity, Entity.type]]
            row.doc.name should be("Devon")
            // No inner-hits opted in → empty map.
            row.innerHits should be(Map.empty[String, List[InnerHit[Any]]])
          }
      }
    }

    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
