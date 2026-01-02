package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.field.Field._
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * Regression: OpenSearch facet queries must behave like Lucene:
 * - values list can be limited (top N buckets)
 * - childCount must still represent the total number of distinct children
 */
@EmbeddedTest
class OpenSearchFacetChildCountSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(tag: String, _id: Id[Doc] = Id[Doc]()) extends Document[Doc]

  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen

    val tag: F[String] = field("tag", _.tag)

    // Single-valued, non-hierarchical facet so the facet-token field is just the label.
    val tagFacet: FF = field.facet("tagFacet", d => List(FacetValue(d.tag)), FacetConfig())
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchFacetChildCountSpec"
    override lazy val directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs = store[Doc, Doc.type](Doc)
  }

  private lazy val db = new DB

  "OpenSearch facets" should {
    "return accurate childCount even when values are limited" in {
      val unique = 25
      val topN = 10

      db.init.next {
        db.docs.transaction { tx =>
          val inserts = (1 to unique).toList.map(i => Doc(tag = f"t$i%03d"))
          tx.truncate.next(tx.insert(inserts)).unit
        }.next {
          db.docs.transaction { tx =>
            tx.query
              .facet(_.tagFacet, childrenLimit = Some(topN))
              .docs
              .limit(1)
              .search
              .map { results =>
                val facet = results.facet(_.tagFacet)
                facet.values.size should be(topN)
                facet.childCount should be(unique)
                facet.totalCount should be(topN) // all buckets have count=1; totalCount is sum of returned bucket counts
              }
          }
        }.guarantee(db.dispose)
      }
    }
  }
}

