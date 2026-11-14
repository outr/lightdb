package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Query, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * A sorted stream longer than its page size reads one consistent set of rows: a row another transaction inserts before
 * the stream's position, or deletes behind it, after the first page has been read, neither repeats a row nor loses
 * one. The stream reads the rows as they were when it began, so the inserted row is not returned and the deleted row,
 * already read, still is. The same holds for a filter on a nested field and for a stream's offset and limit.
 */
trait AbstractStreamSnapshotSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val originals: List[Ranked] = (1 to 30).toList.map { i =>
    val n = i * 10
    Ranked(n, List(Label(if n % 30 == 0 then "other" else "kept")), _id = Id(s"row$n"))
  }
  private val kept: List[Int] = originals.filter(_.labels.exists(_.key == "kept")).map(_.n)
  private val earlier = Ranked(5, List(Label("kept")), _id = Id("row5"))

  private def reseed: Task[Unit] = for {
    _ <- DB.rows.transaction(_.truncate)
    _ <- DB.rows.transaction(_.insert(originals))
  } yield ()

  /** Streams `f`'s query sorted by `n`, running `change` in another transaction once `after` rows have been read. */
  private def streamWith(after: Int, change: Task[Unit])
                        (f: Query[Ranked, Ranked.type, Ranked] => Query[Ranked, Ranked.type, Ranked]): Task[List[Int]] =
    reseed.flatMap { _ =>
      DB.rows.transaction { tx =>
        f(tx.query.sort(Sort.ByField(Ranked.n).ascending)).stream.zipWithIndex.evalMap {
          case (row, index) => (if index == after - 1 then change else Task.unit).map(_ => row.n)
        }.toList
      }
    }

  private def insertEarlier: Task[Unit] = DB.rows.transaction(_.insert(earlier)).unit
  private def deleteFirst: Task[Unit] = DB.rows.transaction(_.delete(Id[Ranked]("row10"))).unit

  private def nestedKept(q: Query[Ranked, Ranked.type, Ranked]): Query[Ranked, Ranked.type, Ranked] =
    q.filter(_.labels.nested(labels => labels.key === "kept"))

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "return every row once when an earlier row is inserted after the first page" in {
      streamWith(10, insertEarlier)(_.pageSize(10)).map(_ should be(originals.map(_.n)))
    }
    "return every row once when a row already read is deleted after the first page" in {
      streamWith(10, deleteFirst)(_.pageSize(10)).map(_ should be(originals.map(_.n)))
    }
    "return the offset and limit's rows once when an earlier row is inserted after the first page" in {
      streamWith(10, insertEarlier)(_.pageSize(10).offset(3).limit(15)).map(_ should be(originals.map(_.n).slice(3, 18)))
    }
    "return the offset and limit's rows once when a row already read is deleted after the first page" in {
      streamWith(10, deleteFirst)(_.pageSize(10).offset(3).limit(15)).map(_ should be(originals.map(_.n).slice(3, 18)))
    }
    "return every nested match once when an earlier row is inserted after the first page" in {
      streamWith(5, insertEarlier)(q => nestedKept(q).pageSize(5)).map(_ should be(kept))
    }
    "return every nested match once when a row already read is deleted after the first page" in {
      streamWith(5, deleteFirst)(q => nestedKept(q).pageSize(5)).map(_ should be(kept))
    }
    "return the nested offset and limit's rows once when an earlier row is inserted after the first page" in {
      streamWith(5, insertEarlier)(q => nestedKept(q).pageSize(5).offset(2).limit(12)).map(_ should be(kept.slice(2, 14)))
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

    val rows: Collection[Ranked, Ranked.type] = store(Ranked)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Label(key: String)

  object Label {
    implicit val rw: RW[Label] = RW.gen
  }

  case class Ranked(n: Int,
                    labels: List[Label],
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Ranked] = Ranked.id()) extends RecordDocument[Ranked]

  object Ranked extends RecordDocumentModel[Ranked] with JsonConversion[Ranked] {
    override implicit val rw: RW[Ranked] = RW.gen

    trait Labels extends Nested[List[Label]] {
      val key: NP[String]
    }

    val n: I[Int] = field.index("n", (d: Ranked) => d.n)
    val labels: N[Labels] = field.index.nested[Labels](_.labels)
  }
}
