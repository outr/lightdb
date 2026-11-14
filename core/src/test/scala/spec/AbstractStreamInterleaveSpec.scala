package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.transaction.CollectionTransaction
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * A stream read while the same transaction runs other operations still returns every row: a get by id, an insert, an
 * upsert, a count or another query run from inside the stream's consumption, over a stream several pages and fetches
 * long, leaves the stream to go on to its last row, and each operation gives its own result. The rows written meanwhile
 * fall outside the stream's filter.
 */
trait AbstractStreamInterleaveSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  protected def rows: Int = 2_500
  private val every = 97

  private type Tx = CollectionTransaction[Counted, Counted.type]

  private def reseed: Task[Unit] = for {
    _ <- DB.rows.transaction(_.truncate)
    _ <- DB.rows.transaction(_.insert((1 to rows).toList.map(i => Counted(i, _id = Id(s"row$i")))))
  } yield ()

  /** Streams the rows with `n >= 1` in order, running `op` on the same transaction at every `every`th row. */
  private def interleaved[T](op: (Tx, Int) => Task[T]): Task[(List[Int], List[T])] = reseed.flatMap { _ =>
    DB.rows.transaction { tx =>
      var results = List.empty[T]
      tx.query.filter(_.n >= 1).sort(Sort.ByField(Counted.n).ascending).stream.evalMap { row =>
        if row.n % every == 0 then op(tx, row.n).map { r =>
          results = r :: results
          row.n
        }
        else Task.pure(row.n)
      }.toList.map(values => (values, results.reverse))
    }
  }

  private val all = (1 to rows).toList
  private val marks = all.filter(_ % every == 0)

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "return every row while the transaction gets other rows by id" in {
      interleaved((tx, n) => tx.get(Id[Counted](s"row${rows + 1 - n}")).map(_.map(_.n))).map {
        case (values, got) =>
          values shouldBe all
          got shouldBe marks.map(n => Some(rows + 1 - n))
      }
    }
    "return every row while the transaction inserts rows" in {
      interleaved((tx, n) => tx.insert(Counted(-n, _id = Id(s"inserted$n"))).map(_.n)).map {
        case (values, inserted) =>
          values shouldBe all
          inserted shouldBe marks.map(-_)
      }
    }
    "return every row while the transaction upserts rows" in {
      interleaved((tx, n) => tx.upsert(Counted(-n, _id = Id(s"upserted$n"))).map(_.n)).map {
        case (values, upserted) =>
          values shouldBe all
          upserted shouldBe marks.map(-_)
      }
    }
    "return every row while the transaction counts" in {
      interleaved((tx, _) => tx.count).map {
        case (values, counts) =>
          values shouldBe all
          counts shouldBe marks.map(_ => rows)
      }
    }
    "return every row while the transaction runs another query" in {
      interleaved((tx, n) => tx.query.filter(_.n <= n).filter(_.n > n - 3).sort(Sort.ByField(Counted.n).ascending).toList.map(_.map(_.n))).map {
        case (values, found) =>
          values shouldBe all
          found shouldBe marks.map(n => List(n - 2, n - 1, n))
      }
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  def storeManager: CollectionManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val rows: Collection[Counted, Counted.type] = store(Counted)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Counted(n: Int,
                     created: Timestamp = Timestamp(),
                     modified: Timestamp = Timestamp(),
                     _id: Id[Counted] = Counted.id()) extends RecordDocument[Counted]

  object Counted extends RecordDocumentModel[Counted] with JsonConversion[Counted] {
    override implicit val rw: RW[Counted] = RW.gen

    val n: I[Int] = field.index("n", (d: Counted) => d.n)
  }
}
