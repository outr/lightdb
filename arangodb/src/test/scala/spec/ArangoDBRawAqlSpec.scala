package spec

import com.arangodb.ArangoDB
import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.arangodb.{ArangoDBStore, ArangoDBStoreManager}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class ArangoDBRawAqlSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with ArangoDBAvailability {
  private val dbName = "ArangoDBRawAqlSpec"

  ArangoDBTestSupport.config.foreach { c =>
    val client = new ArangoDB.Builder().host(c.host, c.port).user(c.user).password(c.password).build()
    try { val db = client.db(dbName); if (db.exists()) db.drop() } finally client.shutdown()
  }

  "ArangoDB raw AQL" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert records" in {
      DB.people.transaction(_.insert(List(
        Person("Adam", 21, Id("adam")),
        Person("Bea", 30, Id("bea")),
        Person("Cam", 41, Id("cam"))
      ))).succeed
    }
    "run a raw AQL query returning projected values" in {
      DB.people.transaction { tx =>
        tx.aql("FOR d IN @@col FILTER d.age >= 30 SORT d.name RETURN d.name").toList.map { names =>
          names.map(_.asString) should be(List("Bea", "Cam"))
        }
      }
    }
    "run a raw AQL query with a bind parameter" in {
      DB.people.transaction { tx =>
        tx.aql("FOR d IN @@col FILTER d.age >= @min RETURN d.age", Map("min" -> 40)).toList.map { ages =>
          ages.map(_.asInt) should be(List(41))
        }
      }
    }
    "run a raw AQL query returning documents" in {
      DB.people.transaction { tx =>
        tx.aqlDocs("FOR d IN @@col SORT d.age RETURN d").toList.map { people =>
          people.map(_.name) should be(List("Adam", "Bea", "Cam"))
        }
      }
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]
  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: I[String] = field.index("name", _.name)
    val age: I[Int] = field.index("age", _.age)
  }

  object DB extends LightDB {
    override type SM = ArangoDBStoreManager
    override val storeManager: ArangoDBStoreManager = ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(dbName))
    override def name: String = dbName
    override lazy val directory: Option[Path] = None
    val people: ArangoDBStore[Person, Person.type] = store(Person)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
