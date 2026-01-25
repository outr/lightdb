package spec

import fabric.*
import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

trait AbstractSpecialCasesSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert a couple SpecialOne instances" in {
      DB.specialOne.transaction(_.insert(List(
        SpecialOne("First", WrappedString("Apple"), Person("Andrew", 1), _id = Id("first")),
        SpecialOne("Second", WrappedString("Banana"), Person("Bianca", 2), _id = Id("second"))
      ))).succeed
    }
    "verify the SpecialOne instances were stored properly" in {
      DB.specialOne.transaction { transaction =>
        transaction.stream.toList.map { list =>
          list.map(_.name).toSet should be(Set("First", "Second"))
          list.map(_.wrappedString).toSet should be(Set(WrappedString("Apple"), WrappedString("Banana")))
          list.map(_.person).toSet should be(Set(Person("Andrew", 1), Person("Bianca", 2)))
          list.map(_._id).toSet should be(Set(SpecialOne.id("first"), SpecialOne.id("second")))
        }
      }
    }
    "verify filtering by created works" in {
      DB.specialOne.transaction { transaction =>
        for
          _ <- transaction.query.filter(_.created < Timestamp()).toList.map(_.map(_.name).toSet should be(Set("First", "Second")))
          _ <- transaction.query.filter(_.created > Timestamp()).toList.map(_.map(_.name).toSet should be(Set.empty))
        yield succeed
      }
    }
    "verify the storage of data is correct" in {
      DB.specialOne.transaction { transaction =>
        transaction.query.sort(Sort.ByField(SpecialOne.name).asc).json(ref => List(ref._id)).toList.map { list =>
          list should be(List(obj("_id" -> str("first")), obj("_id" -> str("second"))))
        }
      }
    }
    "group ids" in {
      DB.specialOne.transaction { transaction =>
        transaction.query.aggregate(ref => List(ref._id.concat)).toList.map { list =>
          list.map(_(_._id.concat)) should be(List(List(SpecialOne.id("first"), SpecialOne.id("second"))))
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

  def storeManager: CollectionManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val specialOne: Collection[SpecialOne, SpecialOne.type] = store(SpecialOne)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class SpecialOne(name: String,
                        wrappedString: WrappedString,
                        person: Person,
                        created: Timestamp = Timestamp(),
                        modified: Timestamp = Timestamp(),
                        _id: Id[SpecialOne] = SpecialOne.id()) extends RecordDocument[SpecialOne]

  object SpecialOne extends RecordDocumentModel[SpecialOne] with JsonConversion[SpecialOne] {
    override implicit val rw: RW[SpecialOne] = RW.gen

    val name: F[String] = field("name", (d: SpecialOne) => d.name)
    val wrappedString: F[WrappedString] = field("wrappedString", (d: SpecialOne) => d.wrappedString)
    val person: F[Person] = field("person", (d: SpecialOne) => d.person)
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
