package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.lucene.LuceneStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class LuceneAutoFieldSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {

  // Document with three fields, but the model only declares 'name'.
  // 'age' and 'color' should be auto-injected at store init.
  case class Widget(name: String,
                    age: Int,
                    color: String = "red",
                    _id: Id[Widget] = Widget.id()) extends Document[Widget]

  object Widget extends DocumentModel[Widget] with JsonConversion[Widget] {
    override implicit val rw: RW[Widget] = RW.gen
    val name: I[String] = field.index(_.name)
    // age and color are intentionally NOT declared
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LuceneAutoFieldSpec"))
    val widgets: Collection[Widget, Widget.type] = store(Widget)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "LuceneAutoFieldSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "store a widget with undeclared fields" in {
      DB.widgets.transaction { tx =>
        tx.insert(Widget("Sprocket", 5, "blue")).map(_ => succeed)
      }
    }
    "retrieve the widget with all fields intact" in {
      DB.widgets.transaction { tx =>
        tx.query.filter(_.name === "Sprocket").toList.map { list =>
          list.length should be(1)
          val w = list.head
          w.name should be("Sprocket")
          w.age should be(5)
          w.color should be("blue")
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
