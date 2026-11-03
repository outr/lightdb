package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A field removed from a model leaves its column and its values in the table: opening the store with the new model
 * keeps every column no field declares ([[SQLStore.retiredColumns]]), so an upgrade run after the open can still read
 * what the old model stored. The raw stream returns a retired column's value beside the declared fields on each row
 * that holds one; a document written since holds none, so its write leaves the column NULL on its row, as a rewritten
 * document loses a field it no longer has. Only [[SQLStore.dropRetiredColumns]] removes a retired column, and it
 * refuses a declared one. Opening again keeps what remains retired.
 */
abstract class AbstractRetiredColumnSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected def storeManager: CollectionManager

  /** Whether the database keeps its tables in [[directory]], cleared before the spec runs. */
  protected def embedded: Boolean = true

  protected lazy val specName: String = getClass.getSimpleName

  private lazy val directory: Path = Path.of(s"db/$specName")

  private val first = RetiredColumnSpec.Before("first", "a note", List("x", "y"), 3, _id = Id("first"))
  private val second = RetiredColumnSpec.Before("second", "another note", Nil, 0, _id = Id("second"))

  private def raw(store: Collection[?, ?]): Task[Map[String, Json]] =
    store.transaction(_.jsonStream.toList).map(_.map(j => j("_id").asString -> j).toMap)

  private def sql(store: Collection[?, ?]): SQLStore[?, ?] = store.asInstanceOf[SQLStore[?, ?]]

  specName should {
    "store records with the fields of the earlier model" in {
      if embedded && Files.exists(directory) then Files.walk(directory).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
      val db = new BeforeDB
      for {
        _ <- db.init
        _ <- db.records.transaction(_.truncate)
        _ <- db.records.transaction(_.upsert(List(first, second)))
        _ <- db.dispose
      } yield succeed
    }
    "open with the later model, keeping the removed fields' columns and values" in {
      val db = new AfterDB
      for {
        _ <- db.init
        rows <- raw(db.records)
        typed <- db.records.transaction(_.list)
        _ <- db.dispose
      } yield {
        sql(db.records).retiredColumns.map(_.toLowerCase).toSet shouldBe Set("note", "tags", "count")
        rows("first").get("note") shouldBe Some(str("a note"))
        rows("first").get("tags") shouldBe Some(arr(str("x"), str("y")))
        rows("first").get("count") shouldBe Some(num(3))
        rows("second").get("note") shouldBe Some(str("another note"))
        rows("first")("title") shouldBe str("first")
        typed.map(_.title).toSet shouldBe Set("first", "second")
      }
    }
    "write a document without the retired columns, leaving them NULL on its row alone" in {
      val db = new AfterDB
      for {
        _ <- db.init
        _ <- db.records.transaction(_.upsert(RetiredColumnSpec.After("first, rewritten", _id = Id("first"))))
        _ <- db.records.transaction(_.insert(RetiredColumnSpec.After("third", _id = Id("third"))))
        rows <- raw(db.records)
        _ <- db.dispose
      } yield {
        rows("first").get("note") shouldBe None
        rows("first").get("tags") shouldBe None
        rows("first")("title") shouldBe str("first, rewritten")
        rows("second").get("note") shouldBe Some(str("another note"))
        rows("third").get("note") shouldBe None
        rows.keySet shouldBe Set("first", "second", "third")
      }
    }
    "drop a retired column only when asked, refusing a declared one, and keep the rest across an open" in {
      val db = new AfterDB
      for {
        _ <- db.init
        refused <- sql(db.records).dropRetiredColumns(List("title")).attempt
        dropped <- sql(db.records).dropRetiredColumns(List("NOTE", "missing"))
        _ <- db.records.transaction(_.upsert(RetiredColumnSpec.After("second, rewritten", _id = Id("second"))))
        afterDrop <- raw(db.records)
        _ <- db.dispose
        again = new AfterDB
        _ <- again.init
        retiredAgain = sql(again.records).retiredColumns.map(_.toLowerCase).toSet
        _ <- again.records.transaction(_.truncate)
        _ <- again.dispose
      } yield {
        refused.isFailure shouldBe true
        dropped.map(_.toLowerCase) shouldBe List("note")
        sql(db.records).retiredColumns.map(_.toLowerCase).toSet shouldBe Set("tags", "count")
        afterDrop.values.flatMap(_.get("note")) shouldBe Nil
        afterDrop("second")("title") shouldBe str("second, rewritten")
        retiredAgain shouldBe Set("tags", "count")
      }
    }
  }

  class BeforeDB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override def name: String = specName
    lazy val directory: Option[Path] = Some(spec.directory)
    val records: Collection[RetiredColumnSpec.Before, RetiredColumnSpec.Before.type] =
      store(RetiredColumnSpec.Before).withName("retired_records")()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  class AfterDB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override def name: String = specName
    lazy val directory: Option[Path] = Some(spec.directory)
    val records: Collection[RetiredColumnSpec.After, RetiredColumnSpec.After.type] =
      store(RetiredColumnSpec.After).withName("retired_records")()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}

object RetiredColumnSpec {
  case class Before(title: String, note: String, tags: List[String], count: Int, _id: Id[Before] = Id()) extends Document[Before]

  object Before extends DocumentModel[Before] with JsonConversion[Before] {
    override implicit val rw: RW[Before] = RW.gen
    val title: F[String] = field("title", _.title)
  }

  case class After(title: String, _id: Id[After] = Id()) extends Document[After]

  object After extends DocumentModel[After] with JsonConversion[After] {
    override implicit val rw: RW[After] = RW.gen
    val title: F[String] = field("title", _.title)
  }
}
