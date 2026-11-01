package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
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
 * A query's offset and limit through its stream: `limit` rows from `offset`, whatever the two are and however they fall across the
 * stream's pages.
 */
trait AbstractQueryOffsetLimitSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private def ordered(f: Query[Numbered, Numbered.type, Numbered] => Query[Numbered, Numbered.type, Numbered]): Task[List[Int]] =
    DB.rows.transaction(tx => f(tx.query.sort(Sort.ByField(Numbered.n).ascending)).toList.map(_.map(_.n)))

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert 120 rows" in {
      DB.rows.transaction(_.insert((1 to 120).toList.map(i => Numbered(i, _id = Id(s"row$i"))))).succeed
    }
    "give the first rows with only a limit" in ordered(_.limit(3)).map(_ should be(List(1, 2, 3)))
    "give a page whose offset is smaller than its limit" in ordered(_.offset(2).limit(5)).map(_ should be(List(3, 4, 5, 6, 7)))
    "give a page whose offset is larger than its limit" in ordered(_.offset(100).limit(3)).map(_ should be(List(101, 102, 103)))
    "give a limit larger than the page size once each, from the start" in {
      ordered(_.pageSize(10).limit(25)).map(_ should be((1 to 25).toList))
    }
    "give a page whose offset is larger than the page size and the limit, across the page size" in {
      ordered(_.pageSize(10).offset(35).limit(12)).map(_ should be((36 to 47).toList))
    }
    "give what is left when the limit runs past the end" in ordered(_.offset(117).limit(10)).map(_ should be(List(118, 119, 120)))
    "give nothing when the offset is past the end" in ordered(_.offset(500).limit(3)).map(_ should be(Nil))
    "give every row from the offset with no limit" in ordered(_.offset(115)).map(_ should be((116 to 120).toList))
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

    val rows: Collection[Numbered, Numbered.type] = store(Numbered)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Numbered(n: Int,
                      created: Timestamp = Timestamp(),
                      modified: Timestamp = Timestamp(),
                      _id: Id[Numbered] = Numbered.id()) extends RecordDocument[Numbered]

  object Numbered extends RecordDocumentModel[Numbered] with JsonConversion[Numbered] {
    override implicit val rw: RW[Numbered] = RW.gen

    val n: I[Int] = field.index("n", (d: Numbered) => d.n)
  }
}
