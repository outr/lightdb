package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.Sort
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

abstract class AbstractNestedSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName

  case class Attr(key: String, percent: Double)
  object Attr {
    implicit val rw: RW[Attr] = RW.gen
  }

  case class Entry(title: String,
                   attrs: List[Attr],
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Entry] = Entry.id()) extends RecordDocument[Entry]
  object Entry extends RecordDocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen

    trait Attrs extends Nested[List[Attr]] {
      val key: NP[String]
      val percent: NP[Double]
    }

    val title: I[String] = field.index(_.title)
    val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))
    override def upgrades: List[DatabaseUpgrade] = Nil
    val entries: Collection[Entry, Entry.type] = store(Entry)
  }

  def storeManager: CollectionManager

  private val d1 = Entry(
    title = "first",
    attrs = List(Attr("tract-a", 0.2), Attr("tract-b", 0.9)),
    _id = Entry.id("d1")
  )
  private val d2 = Entry(
    title = "second",
    attrs = List(Attr("tract-a", 0.8)),
    _id = Entry.id("d2")
  )
  private val d3 = Entry(
    title = "third",
    attrs = List(Attr("tract-c", 0.7)),
    _id = Entry.id("d3")
  )
  private val d4 = Entry(
    title = "fourth",
    attrs = List(Attr("tract-a", 0.6), Attr("tract-z", 0.1)),
    _id = Entry.id("d4")
  )

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert nested docs" in {
      DB.entries.transaction(_.insert(List(d1, d2, d3, d4))).succeed
    }
    "enforce strict same-element semantics (relative fields)" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_.attrs.nested { attrs =>
            attrs.key === "tract-a" && attrs.percent >= 0.5
          })
          .id
          .toList
          .map(_.toSet should be(Set(d2._id, d4._id)))
      }
    }
    "compose nested and outer filters consistently" in {
      DB.entries.transaction { tx =>
        tx.query
          .filter(_.title === "second")
          .filter(_.attrs.nested { attrs =>
            attrs.key === "tract-a" && attrs.percent >= 0.5
          })
          .id
          .toList
          .map(_ should be(List(d2._id)))
      }
    }
    "paginate nested results consistently" in {
      val firstPage = DB.entries.transaction { tx =>
        tx.query
          .filter(_.attrs.nested { attrs =>
            attrs.key === "tract-a" && attrs.percent >= 0.5
          })
          .sort(Sort.IndexOrder)
          .offset(0)
          .limit(1)
          .id
          .toList
      }
      val secondPage = DB.entries.transaction { tx =>
        tx.query
          .filter(_.attrs.nested { attrs =>
            attrs.key === "tract-a" && attrs.percent >= 0.5
          })
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

