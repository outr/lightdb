package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.Sort
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

abstract class AbstractNestedInNestedSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName

  case class Inner(code: String, score: Double)
  object Inner {
    implicit val rw: RW[Inner] = RW.gen
  }

  case class Outer(outerName: String, children: List[Inner])
  object Outer {
    implicit val rw: RW[Outer] = RW.gen
  }

  case class Entry(title: String,
                   outers: List[Outer],
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Entry] = Entry.id()) extends RecordDocument[Entry]
  object Entry extends RecordDocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen

    trait ChildAccess extends Nested[List[Inner]] {
      val code: NP[String]
      val score: NP[Double]
    }

    trait Outers extends Nested[List[Outer]]

    val title: I[String] = field.index(_.title)
    val outers = field.index.nested[Outers](_.outers)
    val childCodeField = field("outers.children.code", _.outers.headOption.flatMap(_.children.headOption.map(_.code)).getOrElse(""))
    val childScoreField = field("outers.children.score", _.outers.headOption.flatMap(_.children.headOption.map(_.score)).getOrElse(0.0))
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override def name: String = specName
    override lazy val directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    val entries: Collection[Entry, Entry.type] = store(Entry)
  }

  def storeManager: CollectionManager

  private val d1 = Entry(
    title = "first",
    outers = List(
      Outer("A", List(Inner("x", 0.2))),
      Outer("B", List(Inner("y", 0.9)))
    ),
    _id = Entry.id("d1")
  )
  private val d2 = Entry(
    title = "second",
    outers = List(
      Outer("A", List(Inner("x", 0.8)))
    ),
    _id = Entry.id("d2")
  )
  private val d3 = Entry(
    title = "third",
    outers = List(
      Outer("C", List(Inner("y", 0.7)))
    ),
    _id = Entry.id("d3")
  )
  private val d4 = Entry(
    title = "fourth",
    outers = List(
      Outer("A", List(Inner("x", 0.6), Inner("z", 0.1)))
    ),
    _id = Entry.id("d4")
  )

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert nested-in-nested docs" in {
      DB.entries.transaction(_.insert(List(d1, d2, d3, d4))).succeed
    }
    "enforce same-outer and same-inner semantics" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ =>
            Filter.Nested[Entry](
              path = "outers",
              filter = Filter.Nested[Entry](
                "children",
                Filter.Equals[Entry, String]("code", "x") && Filter.RangeDouble[Entry]("score", Some(0.5), None)
              )
            )
          )
          .id
          .toList
          .map(_.toSet should be(Set(d2._id, d4._id)))
      }
    }
    "support must-not behavior inside nested-in-nested" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ =>
            Filter.Nested[Entry](
              path = "outers",
              filter = Filter.Nested[Entry](
                "children",
                Filter.Multi[Entry](
                  minShould = 1,
                  filters = List(
                    FilterClause(Filter.Equals[Entry, String]("code", "x"), Condition.Must, None),
                    FilterClause(Filter.RangeDouble[Entry]("score", Some(0.5), None), Condition.Must, None),
                    FilterClause(Filter.Equals[Entry, String]("code", "z"), Condition.MustNot, None)
                  )
                )
              )
            )
          )
          .sort(Sort.IndexOrder)
          .id
          .toList
          .map(ids => ids should be(List(d2._id)))
      }
    }
    "paginate nested-in-nested results consistently" in {
      val firstPage = DB.entries.transaction { tx =>
        tx.query
          .filter(_ =>
            Filter.Nested[Entry](
              path = "outers",
              filter = Filter.Nested[Entry](
                "children",
                Filter.Equals[Entry, String]("code", "x") && Filter.RangeDouble[Entry]("score", Some(0.5), None)
              )
            )
          )
          .sort(Sort.IndexOrder)
          .offset(0)
          .limit(1)
          .id
          .toList
      }
      val secondPage = DB.entries.transaction { tx =>
        tx.query
          .filter(_ =>
            Filter.Nested[Entry](
              path = "outers",
              filter = Filter.Nested[Entry](
                "children",
                Filter.Equals[Entry, String]("code", "x") && Filter.RangeDouble[Entry]("score", Some(0.5), None)
              )
            )
          )
          .sort(Sort.IndexOrder)
          .offset(1)
          .limit(1)
          .id
          .toList
      }
      for
        first <- firstPage
        second <- secondPage
      yield {
        first shouldBe List(d2._id)
        second shouldBe List(d4._id)
      }
    }
    "truncate and dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }
}
