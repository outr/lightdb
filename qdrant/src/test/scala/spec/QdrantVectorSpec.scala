package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.qdrant.{QdrantStore, QdrantStoreManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.vector.VectorMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/** Verifies native Qdrant KNN (vector point + payload), plus the KV contract and hybrid filtering. */
@EmbeddedTest
class QdrantVectorSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with QdrantAvailability {
  private val apple = Id[VDoc]("apple")
  private val almostApple = Id[VDoc]("almostApple")
  private val banana = Id[VDoc]("banana")
  private val car = Id[VDoc]("car")

  private val query = List(1.0, 0.0, 0.0)

  "Qdrant" should {
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
    "count the stored documents" in {
      DB.docs.transaction(_.count).map(_ should be(4))
    }
    "round-trip a document through the point payload" in {
      DB.docs.transaction { tx =>
        tx(apple).map { doc =>
          doc.text should be("apple")
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
    "support a non-KNN filter query (scroll path)" in {
      DB.docs.transaction { tx =>
        tx.query.filter(_.category === "fruit").toList.map { docs =>
          docs.map(_._id).toSet should be(Set(apple, almostApple, banana))
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
    override type SM = QdrantStoreManager
    override val storeManager: QdrantStoreManager = QdrantTestSupport.storeManager
    override def name: String = "QdrantVectorSpec"
    override lazy val directory: Option[Path] = None
    val docs: QdrantStore[VDoc, VDoc.type] = store(VDoc)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
