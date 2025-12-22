package spec

import fabric.rw._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalGroupedAggregateSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager {

  private lazy val specName: String = getClass.getSimpleName

  override def traversalStoreManager: CollectionManager = super.traversalStoreManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = traversalStoreManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val sales: Collection[Sale, Sale.type] = store(Sale)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Sale(category: String,
                  amount: Int,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Sale] = Sale.id()) extends RecordDocument[Sale]

  object Sale extends RecordDocumentModel[Sale] with JsonConversion[Sale] {
    override implicit val rw: RW[Sale] = RW.gen

    val category: F[String] = field("category", _.category)
    val amount: F[Int] = field("amount", _.amount)
  }

  specName should {
    "group aggregates with having + sort" in {
      for {
        _ <- DB.init
        _ <- DB.sales.transaction(_.insert(List(
          Sale("A", 10, _id = Id("a1")),
          Sale("A", 5, _id = Id("a2")),
          Sale("B", 3, _id = Id("b1")),
          Sale("B", 7, _id = Id("b2")),
          Sale("C", 1, _id = Id("c1"))
        )))
        rows <- DB.sales.transaction { tx =>
          tx.query
            .aggregate(m => List(
              m.category.group,      // group key
              m.amount.sum,          // aggregated
              m.amount.count         // aggregated (non-null count)
            ))
            .filter(m => m.category.group !== "C") // HAVING: exclude C group
            .sort(m => m.amount.sum, SortDirection.Descending)
            .toList
        }
      } yield {
        val cats = rows.map(_(_ => Sale.category.group))
        cats shouldBe List("A", "B")
        val sums = rows.map(_(_ => Sale.amount.sum))
        sums shouldBe List(15, 10)
        val counts = rows.map(_(_ => Sale.amount.count))
        counts shouldBe List(2, 2)
      }
    }

    "apply HAVING to global aggregates (single row)" in {
      for {
        _ <- DB.truncate()
        _ <- DB.sales.transaction(_.insert(List(
          Sale("A", 10, _id = Id("a1")),
          Sale("A", 5, _id = Id("a2"))
        )))
        rows1 <- DB.sales.transaction { tx =>
          tx.query
            .aggregate(m => List(m.amount.sum))
            .filter(m => m.amount.sum === 15)
            .toList
        }
        rows2 <- DB.sales.transaction { tx =>
          tx.query
            .aggregate(m => List(m.amount.sum))
            .filter(m => m.amount.sum === 14)
            .toList
        }
      } yield {
        rows1.map(_(_ => Sale.amount.sum)) shouldBe List(15)
        rows2 shouldBe Nil
      }
    }
  }
}


