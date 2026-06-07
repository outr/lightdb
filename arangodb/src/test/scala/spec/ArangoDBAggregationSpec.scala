package spec

import com.arangodb.ArangoDB
import fabric.rw.*
import lightdb.LightDB
import lightdb.arangodb.{ArangoDBStore, ArangoDBStoreManager}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/** Verifies ArangoDB native AQL COLLECT aggregation (grouped sum/count). */
@EmbeddedTest
class ArangoDBAggregationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with ArangoDBAvailability {
  private val dbName = "ArangoDBAggregationSpec"

  ArangoDBTestSupport.config.foreach { c =>
    val client = new ArangoDB.Builder().host(c.host, c.port).user(c.user).password(c.password).build()
    try { val db = client.db(dbName); if (db.exists()) db.drop() } finally client.shutdown()
  }

  "ArangoDB native aggregation" should {
    "initialize" in {
      DB.init.succeed
    }
    "insert sales" in {
      DB.sales.transaction(_.insert(List(
        Sale("A", 10), Sale("A", 5), Sale("B", 7), Sale("C", 3)
      ))).succeed
    }
    "group by category with sum and count (COLLECT)" in {
      DB.sales.transaction { tx =>
        tx.query.aggregate(s => List(s.category.group, s.amount.sum, s.amount.count)).toList.map { rows =>
          rows.map(r => r(_.category.group) -> (r(_.amount.sum), r(_.amount.count))).toMap should be(
            Map("A" -> (15, 2), "B" -> (7, 1), "C" -> (3, 1))
          )
        }
      }
    }
    "compute global min/max/avg/sum (COLLECT)" in {
      DB.sales.transaction { tx =>
        tx.query.aggregate(s => List(s.amount.min, s.amount.max, s.amount.sum)).toList.map { rows =>
          rows.map(r => (r(_.amount.min), r(_.amount.max), r(_.amount.sum))) should be(List((3, 10, 25)))
        }
      }
    }
    "dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  case class Sale(category: String, amount: Int, _id: Id[Sale] = Sale.id()) extends Document[Sale]
  object Sale extends DocumentModel[Sale] with JsonConversion[Sale] {
    override implicit val rw: RW[Sale] = RW.gen
    val category: I[String] = field.index("category", _.category)
    val amount: I[Int] = field.index("amount", _.amount)
  }

  object DB extends LightDB {
    override type SM = ArangoDBStoreManager
    override val storeManager: ArangoDBStoreManager = ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(dbName))
    override def name: String = dbName
    override lazy val directory: Option[Path] = None
    val sales: ArangoDBStore[Sale, Sale.type] = store(Sale)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
