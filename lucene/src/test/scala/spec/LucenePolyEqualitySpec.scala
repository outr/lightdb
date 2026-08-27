package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path
import scala.reflect.ClassTag

/**
 * Equality, inequality, and `in` filters on a field typed against a
 * polymorphic record. The indexer stores such a field as one term
 * holding the value's compact JSON; the query side must render the
 * comparison value the same way.
 */
@EmbeddedTest
class LucenePolyEqualitySpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import PolyShape.given

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LucenePolyEqualitySpec"))
    val items: Collection[PolyItem, PolyItem.type] = store(PolyItem)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "LucenePolyEqualitySpec" should {
    "register the poly subtypes and initialize the database" in {
      PolyShape.register(RW.gen[PolyCircle], RW.static(PolySquare))
      DB.init.succeed
    }
    "store items carrying different poly values" in {
      DB.items.transaction { tx =>
        tx.insert(List(
          PolyItem("small", PolyCircle(1)),
          PolyItem("large", PolyCircle(9)),
          PolyItem("box", PolySquare)
        )).map(_ => succeed)
      }
    }
    "match a case-class poly value by equality" in {
      DB.items.transaction { tx =>
        tx.query.filter(_.shape === PolyCircle(1)).toList.map { list =>
          list.map(_.name) should be(List("small"))
        }
      }
    }
    "match a case-object poly value by equality" in {
      DB.items.transaction { tx =>
        tx.query.filter(_.shape === PolySquare).toList.map { list =>
          list.map(_.name) should be(List("box"))
        }
      }
    }
    "exclude a poly value by inequality" in {
      DB.items.transaction { tx =>
        tx.query.filter(_.shape !== PolySquare).toList.map { list =>
          list.map(_.name).sorted should be(List("large", "small"))
        }
      }
    }
    "match any of several poly values with in" in {
      DB.items.transaction { tx =>
        tx.query.filter(_.shape.in(List(PolyCircle(9), PolySquare))).toList.map { list =>
          list.map(_.name).sorted should be(List("box", "large"))
        }
      }
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}

trait PolyShape

object PolyShape extends PolyType[PolyShape]()(using ClassTag(classOf[PolyShape]))

case class PolyCircle(radius: Int) extends PolyShape

case object PolySquare extends PolyShape

case class PolyItem(name: String, shape: PolyShape, _id: Id[PolyItem] = PolyItem.id()) extends Document[PolyItem]

object PolyItem extends DocumentModel[PolyItem] with JsonConversion[PolyItem] {
  import PolyShape.given

  override implicit val rw: RW[PolyItem] = RW.gen
  val name: I[String] = field.index(_.name)
  val shape: I[PolyShape] = field.index(_.shape)
}
