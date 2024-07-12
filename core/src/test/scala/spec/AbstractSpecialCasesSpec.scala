package spec

import fabric._
import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Path

trait AbstractSpecialCasesSpec extends AnyWordSpec with Matchers {
  spec =>
  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init()
    }
    "insert a couple SpecialOne instances" in {
      DB.specialOne.t.insert(List(
        SpecialOne("First", WrappedString("Apple"), Person("Andrew", 1), _id = Id("first")),
        SpecialOne("Second", WrappedString("Banana"), Person("Bianca", 2), _id = Id("second"))
      ))
    }
    "verify the SpecialOne instances were stored properly" in {
      DB.specialOne.transaction { implicit transaction =>
        val list = DB.specialOne.iterator.toList
        list.map(_.name).toSet should be(Set("First", "Second"))
        list.map(_.wrappedString).toSet should be(Set(WrappedString("Apple"), WrappedString("Banana")))
        list.map(_.person).toSet should be(Set(Person("Andrew", 1), Person("Bianca", 2)))
        list.map(_._id).toSet should be(Set(SpecialOne.id("first"), SpecialOne.id("second")))
      }
    }
    "verify the storage of data is correct" in {
      DB.specialOne.transaction { implicit transaction =>
        val list = DB.specialOne.query.sort(Sort.ByField(SpecialOne.name).asc).search.json(ref => List(ref._id)).list
        list should be(List(obj("_id" -> "first"), obj("_id" -> "second")))
      }
    }
    "group ids" in {
      DB.specialOne.transaction { implicit transaction =>
        val list = DB.specialOne.query.aggregate(ref => List(ref._id.concat)).toList
        list.map(_(_._id.concat)) should be(List(List(SpecialOne.id("first"), SpecialOne.id("second"))))
      }
    }
    "truncate the database" in {
      DB.truncate()
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  def storeManager: StoreManager

  object DB extends LightDB {
    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val specialOne: Collection[SpecialOne, SpecialOne.type] = collection[SpecialOne, SpecialOne.type](SpecialOne)

    override def storeManager: StoreManager = spec.storeManager
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class SpecialOne(name: String,
                        wrappedString: WrappedString,
                        person: Person,
                        created: Long = System.currentTimeMillis(),
                        modified: Long = System.currentTimeMillis(),
                        _id: Id[SpecialOne] = SpecialOne.id()) extends RecordDocument[SpecialOne]

  object SpecialOne extends RecordDocumentModel[SpecialOne] with JsonConversion[SpecialOne] {
    override implicit val rw: RW[SpecialOne] = RW.gen

    val name: F[String] = field("name", _.name)
    val wrappedString: F[WrappedString] = field("wrappedString", _.wrappedString)
    val person: F[Person] = field("person", _.person)
  }

  case class WrappedString(v: String) {
    override def toString: String = s"WrappedString($v)"
  }

  object WrappedString {
    implicit val rw: RW[WrappedString] = RW.string(_.v, s => WrappedString(s))
  }

  case class Person(name: String, age: Int)

  object Person {
    implicit val rw: RW[Person] = RW.gen
  }
}
