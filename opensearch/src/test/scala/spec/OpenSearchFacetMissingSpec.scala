package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.field.Field.*
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

/**
 * Verifies the optional facet "missing bucket" behavior.
 *
 * When enabled, documents that have no facet values for a field contribute to a `$MISSING$` bucket.
 */
@EmbeddedTest
class OpenSearchFacetMissingSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  private val MissingMarker: String = "$MISSING$"

  case class Doc(tags: List[String], _id: Id[Doc] = Id[Doc]()) extends Document[Doc]

  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen

    val tags: F[List[String]] = field("tags", _.tags)

    val tagsFacet: FF = field.facet(
      "tagsFacet",
      d => d.tags.map(t => FacetValue(t)),
      FacetConfig(multiValued = true)
    )
  }

  object db extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchFacetMissingSpec"
    override lazy val directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    // Enable missing-bucket behavior only for this store/model.
    Profig("lightdb.opensearch.Doc.facets.includeMissing").store("true")

    val docs = store[Doc, Doc.type](Doc)
  }

  "OpenSearch facets" should {
    "include a $MISSING$ bucket when enabled" in {
      db.init.next {
        // Indexing and searching must be in separate transactions so the bulk buffer is committed before querying.
        db.docs.transaction { tx =>
          tx.truncate
            .next(tx.upsert(Doc(tags = Nil, _id = Id("missing"))))
            .next(tx.upsert(Doc(tags = List("a"), _id = Id("present"))))
            .unit
        }.next {
          db.docs.transaction { tx =>
            tx.query.facet(_.tagsFacet).docs.search.map { results =>
              val facet = results.facet(_.tagsFacet)
              val values = facet.values.map(v => v.value -> v.count).toMap
              values.get("a") should be(Some(1))
              values.get(MissingMarker) should be(Some(1))
            }
          }
        }.guarantee(db.dispose)
      }
    }
  }
}


