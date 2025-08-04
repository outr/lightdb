package spec

import fabric.rw._
import fabric.str
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.sql.SQLStoreTransaction
import lightdb.sql.query.SQLQuery
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, StoredValue, Timestamp}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

abstract class AbstractSQLSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private val adam = Person("Adam", 21, _id = Person.id("adam"))
  private val brenda = Person("Brenda", 11, _id = Person.id("brenda"))
  private val charlie = Person("Charlie", 35, _id = Person.id("charlie"))

  protected lazy val specName: String = getClass.getSimpleName

  protected var db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "verify the database is empty" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(0))
      }
    }
    "insert the records" in {
      db.people.transaction { transaction =>
        transaction.insert(List(adam, brenda, charlie)).map(_ should not be None)
      }
    }
    "query with an arbitrary query" in {
      db.people.transaction { transaction =>
        val txn = transaction.asInstanceOf[SQLStoreTransaction[Person, Person.type]]
        val query = SQLQuery.parse(
          """SELECT
            | name
            |FROM
            | Person
            |WHERE
            | name = :name""".stripMargin).values("name" -> "Adam")
        txn.search[Name](query).flatMap(_.list).map { names =>
          names should be(List(Name("Adam")))
        }
      }
    }
    "truncate the database" in {
      db.truncate().succeed
    }
    "dispose the database" in {
      db.dispose.succeed
    }
  }

  def storeManager: CollectionManager

  class DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: Collection[Person, Person.type] = store(Person)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    city: Option[City] = None,
                    nicknames: Set[String] = Set.empty,
                    friends: List[Id[Person]] = Nil,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
    val city: I[Option[City]] = field.index(_.city)
    val nicknames: I[Set[String]] = field.index(_.nicknames)
    val friends: I[List[Id[Person]]] = field.index(_.friends)
    val allNames: I[List[String]] = field.index(p => (p.name :: p.nicknames.toList).map(_.toLowerCase))
    val search: T = field.tokenized((doc: Person) => s"${doc.name} ${doc.age}")
    val doc: I[Person] = field.index(identity)
    val ageDouble: I[Double] = field.index(_.age.toDouble)
  }

  case class City(name: String)

  object City {
    implicit val rw: RW[City] = RW.gen
  }

  case class Name(name: String)

  object Name {
    implicit val rw: RW[Name] = RW.gen
  }
}
