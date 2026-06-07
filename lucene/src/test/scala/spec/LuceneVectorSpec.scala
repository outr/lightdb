package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.vector.VectorMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

/** Verifies native Lucene KNN: a KnnFloatVectorField (HNSW) + a KnnFloatVectorQuery for top-k search. */
@EmbeddedTest
class LuceneVectorSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private val apple = Id[VDoc]("apple")
  private val almostApple = Id[VDoc]("almostApple")
  private val banana = Id[VDoc]("banana")
  private val car = Id[VDoc]("car")

  private val query = List(1.0, 0.0, 0.0)

  "Lucene KNN" should {
    "initialize" in {
      DB.init.succeed
    }
    "insert documents with embeddings" in {
      DB.docs.transaction(_.insert(List(
        VDoc("apple", "fruit", List(1.0, 0.0, 0.0), apple),
        VDoc("almost apple", "fruit", List(0.9, 0.1, 0.0), almostApple),
        VDoc("banana", "fruit", List(0.0, 1.0, 0.0), banana),
        VDoc("car", "vehicle", List(0.0, 0.0, 1.0), car)
      ))).map(_ => succeed)
    }
    "round-trip an embedding through the stored value" in {
      DB.docs.transaction { tx =>
        tx(apple).map { doc =>
          doc.embedding should be(List(1.0, 0.0, 0.0))
        }
      }
    }
    "return the nearest neighbors in order (cosine)" in {
      DB.docs.transaction { tx =>
        tx.query.sort(VDoc.embedding.knn(query)).limit(2).toList.map { docs =>
          docs.map(_._id) should be(List(apple, almostApple))
        }
      }
    }
    "support hybrid search (filter + KNN)" in {
      DB.docs.transaction { tx =>
        tx.query.filter(_.category === "vehicle").sort(VDoc.embedding.knn(query)).limit(5).toList.map { docs =>
          docs.map(_._id) should be(List(car))
        }
      }
    }
    "dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  case class VDoc(text: String, category: String, embedding: List[Double], _id: Id[VDoc] = Id()) extends Document[VDoc]

  object VDoc extends DocumentModel[VDoc] with JsonConversion[VDoc] {
    override implicit val rw: RW[VDoc] = RW.gen

    val text: I[String] = field.index("text", _.text)
    val category: I[String] = field.index("category", _.category)
    val embedding: Field.VectorIndex[VDoc] = field.vector("embedding", _.embedding, dimension = 3, metric = VectorMetric.Cosine)
  }

  object DB extends LightDB {
    override type SM = LuceneStore.type
    override val storeManager: LuceneStore.type = LuceneStore
    override def name: String = "LuceneVectorSpec"
    override lazy val directory: Option[java.nio.file.Path] = None
    val docs: lightdb.store.Collection[VDoc, VDoc.type] = store[VDoc, VDoc.type](VDoc)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
