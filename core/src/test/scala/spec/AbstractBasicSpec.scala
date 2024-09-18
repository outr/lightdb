package spec

import fabric.rw._
import lightdb.backup.{DatabaseBackup, DatabaseRestore}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion, MaterializedModel}
import lightdb.feature.DBFeatureKey
import lightdb.filter._
import lightdb.store.StoreManager
import lightdb.transaction.Transaction
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Field, Id, LightDB, Sort, StoredValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import perfolation.double2Implicits

import java.io.File
import java.nio.file.Path

abstract class AbstractBasicSpec extends AnyWordSpec with Matchers { spec =>
  protected def aggregationSupported: Boolean = true
  protected def filterBuilderSupported: Boolean = false
  protected def memoryOnly: Boolean = false

  private val adam = Person("Adam", 21, _id = Person.id("adam"))
  private val brenda = Person("Brenda", 11, _id = Person.id("brenda"))
  private val charlie = Person("Charlie", 35, _id = Person.id("charlie"))
  private val diana = Person("Diana", 15, _id = Person.id("diana"))
  private val evan = Person("Evan", 53, Some(City("Dallas")), _id = Person.id("evan"))
  private val fiona = Person("Fiona", 23, _id = Person.id("fiona"))
  private val greg = Person("Greg", 12, _id = Person.id("greg"))
  private val hanna = Person("Hanna", 62, _id = Person.id("hanna"))
  private val ian = Person("Ian", 89, _id = Person.id("ian"))
  private val jenna = Person("Jenna", 4, _id = Person.id("jenna"))
  private val kevin = Person("Kevin", 33, _id = Person.id("kevin"))
  private val linda = Person("Linda", 72, _id = Person.id("linda"))
  private val mike = Person("Mike", 42, _id = Person.id("mike"))
  private val nancy = Person("Nancy", 22, _id = Person.id("nancy"))
  private val oscar = Person("Oscar", 21, nicknames = Set("Grouchy"), _id = Person.id("oscar"))
  private val penny = Person("Penny", 2, _id = Person.id("penny"))
  private val quintin = Person("Quintin", 99, _id = Person.id("quintin"))
  private val ruth = Person("Ruth", 102, _id = Person.id("ruth"))
  private val sam = Person("Sam", 81, _id = Person.id("sam"))
  private val tori = Person("Tori", 30, nicknames = Set("Nica"), _id = Person.id("tori"))
  private val uba = Person("Uba", 21, _id = Person.id("uba"))
  private val veronica = Person("Veronica", 13, nicknames = Set("Vera", "Nica"), _id = Person.id("veronica"))
  private val wyatt = Person("Wyatt", 30, _id = Person.id("wyatt"))
  private val xena = Person("Xena", 63, _id = Person.id("xena"))
  private val yuri = Person("Yuri", 30, _id = Person.id("yuri"))
  private val zoey = Person("Zoey", 101, _id = Person.id("zoey"))

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori, uba, veronica, wyatt, xena, yuri, zoey
  )

  private var features = Map.empty[DBFeatureKey[Any], Any]
  protected def addFeature[T](key: DBFeatureKey[T], value: T): Unit =
    features += key.asInstanceOf[DBFeatureKey[Any]] -> value

  protected lazy val specName: String = getClass.getSimpleName

  protected var db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init() should be(true)
    }
    "verify the database is empty" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(0)
      }
    }
    "insert the records" in {
      db.people.transaction { implicit transaction =>
        db.people.insert(names) should not be None
      }
    }
    "retrieve the first record by _id -> id" in {
      db.people.transaction { implicit transaction =>
        db.people(_._id -> adam._id) should be(adam)
      }
    }
    "retrieve the first record by id" in {
      db.people.transaction { implicit transaction =>
        db.people(adam._id) should be(adam)
      }
    }
    "count the records in the database" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(26)
      }
    }
    "stream the ids in the database" in {
      db.people.transaction { implicit transaction =>
        val ids = db.people.query.search.id.iterator.toList.toSet
        ids should be(names.map(_._id).toSet)
      }
    }
    "stream the records in the database" in {
      db.people.transaction { implicit transaction =>
        val ages = db.people.iterator.map(_.age).toSet
        ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
      }
    }
    /*"verify the AgeLinks is properly updated" in {
      db.ageLinks.t.get(AgeLinks.id(30)).map(_.people) should be(Some(List(Id("yuri"), Id("wyatt"), Id("tori"))))
    }*/
    "query with aggregate functions" in {
      if (aggregationSupported) {
        db.people.transaction { implicit transaction =>
          val list = db.people.query
            .aggregate(p => List(
              p.age.min,
              p.age.max,
              p.age.avg,
              p.age.sum
            ))
            .toList
          list.map(m => m(_.age.min)).toSet should be(Set(2))
          list.map(m => m(_.age.max)).toSet should be(Set(102))
          list.map(m => m(_.age.avg).f(f = 6)).toSet should be(Set("41.807692"))
          list.map(m => m(_.age.sum)).toSet should be(Set(1087))
        }
      } else {
        succeed
      }
    }
    "search by age range" in {
      db.people.transaction { implicit transaction =>
        val ids = db.people.query.filter(_.age BETWEEN 19 -> 22).search.value(_._id).list
        ids.toSet should be(Set(adam._id, nancy._id, oscar._id, uba._id))
      }
    }
    "search excluding age 30" in {
      db.people.transaction { implicit transaction =>
        val names = db.people.query.filter(_.age !== 30).toList.map(_.name).toSet
        names should be(Set("Linda", "Ruth", "Nancy", "Jenna", "Hanna", "Diana", "Ian", "Zoey", "Quintin", "Uba", "Oscar", "Kevin", "Penny", "Charlie", "Evan", "Sam", "Mike", "Brenda", "Adam", "Xena", "Fiona", "Greg", "Veronica"))
      }
    }
    "sort by age" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.sort(Sort.ByField(Person.age).descending).search.docs.list
        people.map(_.name).take(3) should be(List("Ruth", "Zoey", "Quintin"))
      }
    }
    "group by age" in {
      db.people.transaction { implicit transaction =>
        val list = db.people.query.grouped(_.age).toList
        list.map(_._1) should be(List(2, 4, 11, 12, 13, 15, 21, 22, 23, 30, 33, 35, 42, 53, 62, 63, 72, 81, 89, 99, 101, 102))
        list.map(_._2.map(_.name).toSet) should be(List(
          Set("Penny"), Set("Jenna"), Set("Brenda"), Set("Greg"), Set("Veronica"), Set("Diana"),
          Set("Adam", "Uba", "Oscar"), Set("Nancy"), Set("Fiona"), Set("Tori", "Yuri", "Wyatt"), Set("Kevin"),
          Set("Charlie"), Set("Mike"), Set("Evan"), Set("Hanna"), Set("Xena"), Set("Linda"), Set("Sam"), Set("Ian"),
          Set("Quintin"), Set("Zoey"), Set("Ruth")
        ))
      }
    }
    "delete some records" in {
      db.people.transaction { implicit transaction =>
        db.people.delete(_._id -> linda._id) should be(true)
        db.people.delete(_._id -> yuri._id) should be(true)
      }
    }
    "query with multiple nots" in {
      db.people.transaction { implicit transaction =>
        val query = db.people.query.filter { ref =>
          ref.builder
            .mustNot(_.age < 30)
            .mustNot(_.age > 35)
        }
        val list = query.toList
        list.map(_.name).toSet should be(Set("Charlie", "Kevin", "Tori", "Wyatt"))
      }
    }
    "verify the records were deleted" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(24)
      }
    }
    "modify a record" in {
      db.people.transaction { implicit transaction =>
        db.people.modify(adam._id) {
          case Some(p) => Some(p.copy(name = "Allan"))
          case None => fail("Adam was not found!")
        }
      } match {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      db.people.transaction { implicit transaction =>
        db.people(_._id -> adam._id).name should be("Allan")
      }
    }
    "verify start time has been set" in {
      db.startTime.get() should be > 0L
    }
    "dispose the database and prepare new instance" in {
      if (memoryOnly) {
        // Don't dispose
      } else {
        db.dispose()
        db = new DB
        db.init()
      }
    }
    "query the database to verify records were persisted properly" in {
      db.people.transaction { implicit transaction =>
        db.people.iterator.toList.map(_.name).toSet should be(Set(
          "Tori", "Ruth", "Nancy", "Jenna", "Hanna", "Wyatt", "Diana", "Ian", "Quintin", "Uba", "Oscar", "Kevin",
          "Penny", "Charlie", "Evan", "Sam", "Mike", "Brenda", "Zoey", "Allan", "Xena", "Fiona", "Greg", "Veronica"
        ))
      }
    }
    "search using tokenized data and a parsed query" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.filter(_.search.words("nica 13")).toList
        people.map(_.name) should be(List("Veronica"))
      }
    }
    "search using Filter.Builder and scoring" in {
      if (filterBuilderSupported) {
        db.people.transaction { implicit transaction =>
          val results = db.people.query.scored.filter(_
            .builder
            .minShould(0)
            .should(_.search.words("nica 13"), boost = Some(2.0))
            .should(_.age <=> (10, 15))
          ).search.docs
          val people = results.list
          people.map(_.name) should be(List("Veronica", "Brenda", "Diana", "Greg", "Charlie", "Evan", "Fiona", "Hanna", "Ian", "Jenna", "Kevin", "Mike", "Nancy", "Oscar", "Penny", "Quintin", "Ruth", "Sam", "Tori", "Uba", "Wyatt", "Xena", "Zoey", "Allan"))
          results.scores should be(List(4.660672187805176, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
        }
      }
    }
    "search where city is not set" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.filter(_.city === None).toList
        people.map(_.name).toSet should be(Set("Tori", "Ruth", "Sam", "Nancy", "Jenna", "Hanna", "Wyatt", "Diana", "Ian", "Quintin", "Uba", "Oscar", "Kevin", "Penny", "Charlie", "Mike", "Brenda", "Zoey", "Allan", "Xena", "Fiona", "Greg", "Veronica"))
      }
    }
    "search where city is set" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.filter(_.builder.mustNot(_.city === None)).toList
        people.map(_.name) should be(List("Evan"))
      }
    }
    "update the city for a user" in {
      db.people.transaction { implicit transaction =>
        val p = db.people(zoey._id)
        db.people.upsert(p.copy(city = Some(City("Los Angeles"))))
      }
    }
    "modify a record within a transaction and see it post-commit" in {
      db.people.transaction { implicit transaction =>
        val original = db.people.query.filter(_.name === "Ruth").toList.head
        db.people.upsert(original.copy(
          name = "Not Ruth"
        ))
        transaction.commit()
        val people = db.people.query.filter(_.name === "Not Ruth").toList
        people.map(_.name) should be(List("Not Ruth"))
      }
    }
    "query with single-value nicknames" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.filter(_.nicknames has "Grouchy").toList
        people.map(_.name) should be(List("Oscar"))
      }
    }
    "query with multi-value nicknames" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query
          .filter(_.nicknames has "Nica")
          .filter(_.nicknames has "Vera")
          .toList
        people.map(_.name) should be(List("Veronica"))
      }
    }
    "query name with regex match" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query.filter(_.name ~* "Han.+").toList
        people.map(_.name) should be(List("Hanna"))
      }
    }
    "query nicknames with regex match" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query
          .filter(_.nicknames ~* ".+chy")
          .toList
        people.map(_.name) should be(List("Oscar"))
      }
    }
    "query with single-value, multiple nicknames" in {
      db.people.transaction { implicit transaction =>
        val people = db.people.query
          .filter(_.nicknames has "Nica")
          .toList
        people.map(_.name).toSet should be(Set("Veronica", "Tori"))
      }
    }
    "sort by name and page through results" in {
      db.people.transaction { implicit transaction =>
        val q = db.people.query.sort(Sort.ByField(Person.name)).limit(10)
        q.offset(0).search.docs.list.map(_.name) should be(List("Allan", "Brenda", "Charlie", "Diana", "Evan", "Fiona", "Greg", "Hanna", "Ian", "Jenna"))
        q.offset(10).search.docs.list.map(_.name) should be(List("Kevin", "Mike", "Nancy", "Not Ruth", "Oscar", "Penny", "Quintin", "Sam", "Tori", "Uba"))
        q.offset(20).search.docs.list.map(_.name) should be(List("Veronica", "Wyatt", "Xena", "Zoey"))
      }
    }
    "do a database backup" in {
      DatabaseBackup.archive(db, new File(s"backups/$specName.zip")) should be(27)
    }
    "insert a lot more names" in {
      db.people.transaction { implicit transaction =>
        val p = (1 to 1_000).toList.map { index =>
          Person(
            name = s"Unique Snowflake $index",
            age = if (index > 100) 0 else index,
            city = Some(City("Robotland")),
            nicknames = Set("robot", s"sf$index")
          )
        }
        db.people.insert(p)
      }
    }
    "verify the correct number of people exist in the database" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(1_024)
      }
    }
    "verify the correct count in query total" in {
      db.people.transaction { implicit transaction =>
        val results = db.people.query
          .filter(_.nicknames has "robot")
          .sort(Sort.ByField(Person.age).descending)
          .limit(100)
          .countTotal(true)
          .search
          .docs
        results.list.length should be(100)
        results.total should be(Some(1_000))
        results.remaining should be(Some(1_000))
      }
    }
    "verify the correct count in query total with offset" in {
      db.people.transaction { implicit transaction =>
        val results = db.people.query
          .filter(_.nicknames has "robot")
          .limit(100)
          .offset(100)
          .countTotal(true)
          .search
          .docs
        results.list.length should be(100)
        results.total should be(Some(1_000))
        results.remaining should be(Some(900))
      }
    }
    "truncate the collection" in {
      db.people.transaction { implicit transaction =>
        db.people.truncate() should be(1_024)
      }
    }
    "verify the collection is empty" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(0)
      }
    }
    "restore from database backup" in {
      DatabaseRestore.archive(db, new File(s"backups/$specName.zip")) should be(27)
    }
    "verify the correct number of records exist" in {
      db.people.transaction { implicit transaction =>
        db.people.count should be(24)
      }
    }
    "truncate the collection again" in {
      db.people.transaction { implicit transaction =>
        db.people.truncate() should be(24)
      }
    }
    "dispose the database" in {
      db.dispose()
    }
  }

  def storeManager: StoreManager

  class DB extends LightDB {
    spec.features.foreach {
      case (key, value) =>
        put(key, value)
    }

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

    val people: Collection[Person, Person.type] = collection(Person)
    // TODO: Revisit this - performance is currently awful and transaction state causes overlapping dirty data
//    val ageLinks: Collection[AgeLinks, AgeLinks.type] = collection(AgeLinks)

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String,
                    age: Int,
                    city: Option[City] = None,
                    nicknames: Set[String] = Set.empty,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = field.index("name", _.name)
    val age: I[Int] = field.index("age", _.age)
    val city: I[Option[City]] = field.index("city", _.city)
    val nicknames: I[Set[String]] = field.index("nicknames", _.nicknames)
    val search: T = field.tokenized("search", doc => s"${doc.name} ${doc.age}")
  }

  case class City(name: String)

  object City {
    implicit val rw: RW[City] = RW.gen
  }

  /*case class AgeLinks(age: Int, people: List[Id[Person]]) extends Document[AgeLinks] {
    lazy val _id: Id[AgeLinks] = AgeLinks.id(age)
  }

  object AgeLinks extends MaterializedModel[AgeLinks, Person, Person.type] with JsonConversion[AgeLinks] {
    override implicit val rw: RW[AgeLinks] = RW.gen

    val age: F[Int] = field("age", _.age)
    val people: F[List[Id[Person]]] = field("people", _.people)

    override def materialCollection: Collection[Person, Person.type] = db.people

    def id(age: Int): Id[AgeLinks] = Id(age.toString)

    override protected def adding(doc: Person)(implicit transaction: Transaction[Person]): Unit = db.ageLinks.t.modify(AgeLinks.id(doc.age)) {
      case Some(links) => Some(links.copy(people = (doc._id :: links.people).distinct))
      case None => Some(AgeLinks(doc.age, List(doc._id)))
    }
    override protected def modifying(oldDoc: Person, newDoc: Person)(implicit transaction: Transaction[Person]): Unit = adding(newDoc)
    override protected def removing(doc: Person)(implicit transaction: Transaction[Person]): Unit = db.ageLinks.t.modify(AgeLinks.id(doc.age)) {
      case Some(links) =>
        val l = links.copy(people = links.people.filterNot(_ == doc._id))
        if (l.people.isEmpty) {
          None
        } else {
          Some(l)
        }
      case None => None
    }
  }*/

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): Unit = db.startTime.set(System.currentTimeMillis())
  }
}
