package spec

import fabric.rw.*
import lightdb.LightDB
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

/**
 * Regression suite for legal-like nested filtering semantics used by LogicalNetwork.
 *
 * This intentionally mirrors LegalFilter behavior:
 * - optional range clause on section
 * - optional exact township/range clauses
 * - all active clauses must match within the same nested legal element
 */
abstract class AbstractNestedLegalFilterSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName

  case class Legal(section: Int, township: String, range: String)
  object Legal {
    implicit val rw: RW[Legal] = RW.gen
  }

  case class IntRangeFilter(min: Option[Int] = None, max: Option[Int] = None)

  case class Entry(name: String,
                   legals: List[Legal],
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Entry] = Entry.id()) extends RecordDocument[Entry]
  object Entry extends RecordDocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen

    trait Legals extends Nested[List[Legal]] {
      val section: NP[Int]
      val township: NP[String]
      val range: NP[String]
    }

    val name: I[String] = field.index(_.name)
    val legals: N[Legals] = field.index.nested[Legals](_.legals, indexParent = false)
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
    name = "A",
    legals = List(
      Legal(section = 12, township = "19S", range = "03W"),
      Legal(section = 7, township = "20S", range = "04W")
    ),
    _id = Entry.id("d1")
  )
  private val d2 = Entry(
    name = "B",
    legals = List(Legal(section = 13, township = "19S", range = "03W")),
    _id = Entry.id("d2")
  )
  private val d3 = Entry(
    name = "C",
    legals = List(Legal(section = 12, township = "18S", range = "03W")),
    _id = Entry.id("d3")
  )
  private val d4 = Entry(
    name = "D",
    legals = List(Legal(section = 5, township = "17S", range = "01W")),
    _id = Entry.id("d4")
  )

  private def intRange(field: Entry.NP[Int], range: IntRangeFilter): Option[Filter[Entry]] =
    if (range.min.isEmpty && range.max.isEmpty) None else Some(field.range(range.min, range.max))

  private def legalFilter(section: Option[IntRangeFilter] = None,
                          township: Option[String] = None,
                          range: Option[String] = None): Filter[Entry] = {
    val hasCriteria = section.nonEmpty || township.exists(_.trim.nonEmpty) || range.exists(_.trim.nonEmpty)
    if (!hasCriteria) {
      Filter.Multi[Entry](minShould = 0)
    } else {
      Entry.legals.nested { legal =>
        val clauses = List(
          section.flatMap(intRange(legal.section, _)),
          township.map(t => legal.township === t.trim.toUpperCase),
          range.map(r => legal.range === r.trim.toUpperCase)
        ).flatten
        clauses.reduce(_ && _)
      }
    }
  }

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }

    "insert test docs" in {
      DB.entries.transaction(_.insert(List(d1, d2, d3, d4))).succeed
    }

    "match by township only (regression for zero results bug)" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ => legalFilter(township = Some("19S")))
          .id
          .toList
          .map(_.toSet should be(Set(d1._id, d2._id)))
      }
    }

    "match inclusive exact section when min == max" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ => legalFilter(section = Some(IntRangeFilter(min = Some(12), max = Some(12)))))
          .id
          .toList
          .map(_.toSet should be(Set(d1._id, d3._id)))
      }
    }

    "match section range 11..13 inclusively" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ => legalFilter(section = Some(IntRangeFilter(min = Some(11), max = Some(13)))))
          .id
          .toList
          .map(_.toSet should be(Set(d1._id, d2._id, d3._id)))
      }
    }

    "enforce same-element semantics across multiple legal clauses" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_ => legalFilter(section = Some(IntRangeFilter(min = Some(12), max = Some(12))), township = Some("19S"), range = Some("03W")))
          .id
          .toList
          .map(_ should be(List(d1._id)))
      }
    }

    "truncate and dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }
}
