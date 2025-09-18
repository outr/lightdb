package spec

import fabric.rw._
import fabric.str
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.sql.SQLStoreTransaction
import lightdb.sql.query.SQLQuery
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{CompositeIndex, LightDB, StoredValue}
import lightdb.time.Timestamp
import lightdb.filter._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractSQLSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private val adam = Person(
    name = "Adam",
    organDonor = false,
    age = 21,
    gender = Gender.Male,
    city = Some(City("Somewhere")),
    _id = Person.id("adam")
  )
  private val brenda = Person(
    name = "Brenda",
    organDonor = true,
    age = 11,
    gender = Gender.Female,
    _id = Person.id("brenda")
  )
  private val charlie = Person(
    name = "Charlie",
    organDonor = false,
    age = 35,
    gender = Gender.Male,
    _id = Person.id("charlie")
  )

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
        /*
          // TODO: Support DSL for query building
          txn.sql
            .columns(p => p.name)
            .where(p => p.name === "Adam")
            .as[Name]
            .stream
         */
        txn.sql[Name](s"SELECT name FROM ${txn.fqn} WHERE name = :name") { q =>
          q.values("name" -> "Adam")
        }.flatMap(_.list).map { names =>
          names should be(List(Name("Adam")))
        }
      }
    }
    "verify generated SQL query contains no limit" in {
      db.people.transaction { transaction =>
        Task {
          val txn = transaction.asInstanceOf[SQLStoreTransaction[Person, Person.type]]
          val sql = txn
            .toSQL(txn.query.clearPageSize).query.query
            .replaceAll("\\s+", " ")
            .replaceAll("\\S+Person", "Person")
            .trim
          sql should be("SELECT _id, created, modified, name, organDonor, age, gender, city, nicknames, friends, allNames, search, doc, ageDouble FROM Person")
        }
      }
    }
    "verify queryFull populates arguments" in {
      db.people.transaction { transaction =>
        Task {
          val txn = transaction.asInstanceOf[SQLStoreTransaction[Person, Person.type]]
          val sql = txn
            .toSQL(txn.query.clearPageSize.filter(p => p.name === "Adam" && p.age === 21 && p.city === Some(City("Somewhere")))).query.queryLiteral
            .replaceAll("\\s+", " ")
            .replaceAll("\\S+Person", "Person")
            .trim
          sql should be("""SELECT _id, created, modified, name, organDonor, age, gender, city, nicknames, friends, allNames, search, doc, ageDouble FROM Person WHERE name = 'Adam' AND age = 21 AND city = '{\"name\":\"Somewhere\"}'""")
        }
      }
    }
    "get Adam directly" in {
      db.people.transaction { transaction =>
        transaction(adam._id).map { person =>
          person.age should be(21)
          person.gender should be(Gender.Male)
        }
      }
    }
    "query with multiple args" in {
      db.people.transaction { transaction =>
        val txn = transaction.asInstanceOf[SQLStoreTransaction[Person, Person.type]]
        txn.sql[Name](s"SELECT name FROM ${txn.fqn} WHERE name IN (:names)") { query =>
          query.fillPlaceholder("names", "Adam".json, "Charlie".json)
        }.flatMap(_.list).map { people =>
          people.map(_.name).toSet should be(Set("Adam", "Charlie"))
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

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: Collection[Person, Person.type] = store(Person)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    organDonor: Boolean,
                    age: Int,
                    gender: Gender,
                    city: Option[City] = None,
                    nicknames: Set[String] = Set.empty,
                    friends: List[Id[Person]] = Nil,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = field.index(_.name)
    val organDonor: I[Boolean] = field.index(_.organDonor)
    val age: I[Int] = field.index(_.age)
    val gender: I[Gender] = field.index(_.gender)
    val city: I[Option[City]] = field.index(_.city)
    val nicknames: I[Set[String]] = field.index(_.nicknames)
    val friends: I[List[Id[Person]]] = field.index(_.friends)
    val allNames: I[List[String]] = field.index(p => (p.name :: p.nicknames.toList).map(_.toLowerCase))
    val search: T = field.tokenized((doc: Person) => s"${doc.name} ${doc.age}")
    val doc: I[Person] = field.index(p => p)
    val ageDouble: I[Double] = field.index(_.age.toDouble)

    val ageAndGender: CompositeIndex[Person] = field.indexComposite(fields = List(age, gender))
  }

  sealed trait Gender

  object Gender {
    implicit val rw: RW[Gender] = RW.enumeration(List(Male, Female))

    case object Male extends Gender
    case object Female extends Gender
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
