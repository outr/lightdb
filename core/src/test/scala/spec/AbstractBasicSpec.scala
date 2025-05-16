package spec

import fabric.rw._
import lightdb.backup.{DatabaseBackup, DatabaseRestore}
import lightdb.doc._
import lightdb.feature.DBFeatureKey
import lightdb.filter._
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager, Store}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, StoredValue, Timestamp}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import perfolation.double2Implicits
import rapid.{AsyncTaskSpec, Task}

import java.io.File
import java.nio.file.Path

abstract class AbstractBasicSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  val CreateRecords = 10_000

  private val start = Timestamp()

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
  private val sam = Person("Sam", 81, friends = List(adam._id, fiona._id), _id = Person.id("sam"))
  private val tori = Person("Tori", 30, nicknames = Set("Nica"), _id = Person.id("tori"))
  private val uba = Person("Uba", 21, nicknames = Set("multiple terms"), _id = Person.id("uba"))
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
      db.init.succeed
    }
    "verify the database is empty" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(0))
      }
    }
    "insert the records" in {
      db.people.transaction { transaction =>
        transaction.insert(names).map(_ should not be None)
      }
    }
    "retrieve the first record by _id -> id" in {
      db.people.transaction { transaction =>
        transaction(_._id -> adam._id).map(_ should be(adam))
      }
    }
    "retrieve the first record by id" in {
      db.people.transaction { transaction =>
        transaction(adam._id).map(_ should be(adam))
      }
    }
    "count the records in the database" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(26))
      }
    }
    "stream the ids in the database" in {
      db.people.transaction { transaction =>
        transaction.query.id.stream.toList.map(_.toSet).map { ids =>
          ids should be(names.map(_._id).toSet)
        }
      }
    }
    "stream the records in the database" in {
      db.people.transaction { transaction =>
        transaction.stream.map(_.age).toList.map(_.toSet).map { ages =>
          ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
        }
      }
    }
    "verify starts with matches" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name.startsWith("Ver")).toList.map { list =>
          list.map(_.name) should be(List("Veronica"))
        }
      }
    }
    "verify ends with matches" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name.endsWith("nica")).toList.map { list =>
          list.map(_.name) should be(List("Veronica"))
        }
      }
    }
    "verify exactly matches" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name.exactly("Veronica")).toList.map { list =>
          list.map(_.name) should be(List("Veronica"))
        }
      }
    }
    "verify regex matches" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name.regex(".*ron.*")).toList.map { list =>
          list.map(_.name) should be(List("Veronica"))
        }
      }
    }
    "verify the AgeLinks is properly updated" in {
      db.ageLinks.t.get(AgeLinks.id(30)).map(_.map(_.people).map(_.toSet) should be(Some(Set(Id("yuri"), Id("wyatt"), Id("tori")))))
    }
    "query with aggregate functions" in {
      if (aggregationSupported) {
        db.people.transaction { transaction =>
          transaction.query
            .aggregate(p => List(
              p.age.min,
              p.age.max,
              p.age.avg,
              p.age.sum
            ))
            .toList
            .map { list =>
              list.map(m => m(_.age.min)).toSet should be(Set(2))
              list.map(m => m(_.age.max)).toSet should be(Set(102))
              list.map(m => m(_.age.avg).f(f = 6)).toSet should be(Set("41.807692"))
              list.map(m => m(_.age.sum)).toSet should be(Set(1087))
            }
        }
      } else {
        succeed
      }
    }
    "search by age range" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.age BETWEEN 19 -> 22).value(_._id).stream.toList.map { ids =>
          ids.toSet should be(Set(adam._id, nancy._id, oscar._id, uba._id))
        }
      }
    }
    "search excluding age 30" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.age !== 30).toList.map(_.map(_.name).toSet).map { names =>
          names should be(Set("Linda", "Ruth", "Nancy", "Jenna", "Hanna", "Diana", "Ian", "Zoey", "Quintin", "Uba", "Oscar", "Kevin", "Penny", "Charlie", "Evan", "Sam", "Mike", "Brenda", "Adam", "Xena", "Fiona", "Greg", "Veronica"))
        }
      }
    }
    "sort by age" in {
      db.people.transaction { transaction =>
        transaction.query.sort(Sort.ByField(Person.age).descending).toList.map { people =>
          people.map(_.name).take(3) should be(List("Ruth", "Zoey", "Quintin"))
        }
      }
    }
    "group by age" in {
      db.people.transaction { transaction =>
        transaction.query.grouped(_.age).toList.map { list =>
          list.map(_.group) should be(List(2, 4, 11, 12, 13, 15, 21, 22, 23, 30, 33, 35, 42, 53, 62, 63, 72, 81, 89, 99, 101, 102))
          list.map(_.results.map(_.name).toSet) should be(List(
            Set("Penny"), Set("Jenna"), Set("Brenda"), Set("Greg"), Set("Veronica"), Set("Diana"),
            Set("Adam", "Uba", "Oscar"), Set("Nancy"), Set("Fiona"), Set("Tori", "Yuri", "Wyatt"), Set("Kevin"),
            Set("Charlie"), Set("Mike"), Set("Evan"), Set("Hanna"), Set("Xena"), Set("Linda"), Set("Sam"), Set("Ian"),
            Set("Quintin"), Set("Zoey"), Set("Ruth")
          ))
        }
      }
    }
    "delete some records" in {
      db.people.transaction { transaction =>
        transaction.delete(_._id -> linda._id).and(transaction.delete(_._id -> yuri._id)).map { t =>
          t should be(true -> true)
        }
      }
    }
    "query with multiple nots" in {
      db.people.transaction { transaction =>
        val query = transaction.query.filter { ref =>
          ref.builder
            .mustNot(_.age < 30)
            .mustNot(_.age > 35)
        }
        query.toList.map { list =>
          list.map(_.name).toSet should be(Set("Charlie", "Kevin", "Tori", "Wyatt"))
        }
      }
    }
    "verify the records were deleted" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(24))
      }
    }
    "modify a record" in {
      db.people.transaction { transaction =>
        transaction.modify(adam._id) {
          case Some(p) => Task.pure(Some(p.copy(name = "Allan")))
          case None => fail("Adam was not found!")
        }
      }.map {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      db.people.transaction { transaction =>
        transaction(_._id -> adam._id).map(_.name should be("Allan"))
      }
    }
    "verify start time has been set" in {
      db.startTime.get().map(_ should be > 0L)
    }
    "dispose the database and prepare new instance" in {
      if (memoryOnly) {
        // Don't dispose
        Task.unit.succeed
      } else {
        db.dispose.next {
          db = new DB
          db.init
        }.succeed
      }
    }
    "query the database to verify records were persisted properly" in {
      db.people.transaction { transaction =>
        transaction.stream.toList.map(_.map(_.name).toSet).map { set =>
          set should be(Set(
            "Tori", "Ruth", "Nancy", "Jenna", "Hanna", "Wyatt", "Diana", "Ian", "Quintin", "Uba", "Oscar", "Kevin",
            "Penny", "Charlie", "Evan", "Sam", "Mike", "Brenda", "Zoey", "Allan", "Xena", "Fiona", "Greg", "Veronica"
          ))
        }
      }
    }
    "search using tokenized data and a parsed query" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.search.words("nica 13", matchEndsWith = true)).toList.map { people =>
          people.map(_.name) should be(List("Veronica"))
        }
      }
    }
    "search using Filter.Builder and scoring" in {
      if (filterBuilderSupported) {
        db.people.transaction { transaction =>
          transaction.query.scored.filter(_
            .builder
            .minShould(0)
            .should(_.search.words("nica 13", matchEndsWith = true), boost = Some(2.0))
            .should(_.age <=> (10, 15))
          ).docs.search.flatMap { results =>
            for {
              people <- results.list
              scores <- results.scores
            } yield {
              people.map(_.name) should be(List("Veronica", "Brenda", "Diana", "Greg", "Charlie", "Evan", "Fiona", "Hanna", "Ian", "Jenna", "Kevin", "Mike", "Nancy", "Oscar", "Penny", "Quintin", "Ruth", "Sam", "Tori", "Uba", "Wyatt", "Xena", "Zoey", "Allan"))
              scores should be(List(6.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
            }
          }
        }
      } else {
        Task.unit.succeed
      }
    }
    "search where city is not set" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.city === None).toList.map { people =>
          people.map(_.name).toSet should be(Set("Tori", "Ruth", "Sam", "Nancy", "Jenna", "Hanna", "Wyatt", "Diana", "Ian", "Quintin", "Uba", "Oscar", "Kevin", "Penny", "Charlie", "Mike", "Brenda", "Zoey", "Allan", "Xena", "Fiona", "Greg", "Veronica"))
        }
      }
    }
    "search where city is set" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.builder.mustNot(_.city === None)).toList.map { people =>
          people.map(_.name) should be(List("Evan"))
        }
      }
    }
    "update the city for a user" in {
      db.people.transaction { transaction =>
        transaction(zoey._id).flatMap { p =>
          transaction.upsert(p.copy(city = Some(City("Los Angeles"))))
        }
      }.succeed
    }
    "modify a record within a transaction and see it post-commit" in {
      db.people.transaction { transaction =>
        for {
          original <- transaction.query.filter(_.name === "Ruth").first
          _ <- transaction.upsert(original.copy(
            name = "Not Ruth"
          ))
          _ <- transaction.commit
          people <- transaction.query.filter(_.name === "Not Ruth").toList
        } yield people.map(_.name) should be(List("Not Ruth"))
      }
    }
    "query with single-value nicknames" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.nicknames has "Grouchy").toList.map { people =>
          people.map(_.name) should be(List("Oscar"))
        }
      }
    }
    "query with indexes" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name in List("Allan", "Brenda", "Charlie")).indexes.search.flatMap(_.list).map { results =>
          results.map(_(_.name)).toSet should be(Set("Allan", "Brenda", "Charlie"))
          results.map(_(_.doc).name).toSet should be(Set("Allan", "Brenda", "Charlie"))
        }
      }
    }
    "query with doc and indexes" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name in List("Allan", "Brenda", "Charlie")).docAndIndexes.stream.toList.map { results =>
          results.map(_(_.name)).toSet should be(Set("Allan", "Brenda", "Charlie"))
          results.map(_.doc.name).toSet should be(Set("Allan", "Brenda", "Charlie"))
        }
      }
    }
    "query with multi-value nicknames" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames has "Nica")
          .filter(_.nicknames has "Vera")
          .toList
          .map { people =>
            people.map(_.name) should be(List("Veronica"))
          }
      }
    }
    "query name with regex match" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name ~* "Han.+").toList.map { people =>
          people.map(_.name) should be(List("Hanna"))
        }
      }
    }
    "query nicknames that contain ica" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames.contains("ica"))
          .toList
          .map { people =>
            people.map(_.name).toSet should be(Set("Tori", "Veronica"))
          }
      }
    }
    // TODO: Fix support in SQL
    /*"query all names that start with t" in {
      db.people.transaction { transaction =>
        val people = transaction.query.filter(_.allNames.startsWith("t")).toList
        people.map(_.name).toSet should be(Set("Tori"))
      }
    }*/
    "query nicknames with regex match" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames ~* ".+chy")
          .toList
          .map { people =>
            people.map(_.name) should be(List("Oscar"))
          }
      }
    }
    "materialize empty nicknames" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.name === "Ian").materialized(p => List(p.nicknames)).toList.map { people =>
          people.map(m => m(_.nicknames)) should be(List(Set.empty))
        }
      }
    }
    "query with single-value, multiple nicknames" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames has "Nica")
          .toList
          .map { people =>
            people.map(_.name).toSet should be(Set("Veronica", "Tori"))
          }
      }
    }
    "sort by name and page through results" in {
      db.people.transaction { transaction =>
        val q = transaction.query.sort(Sort.ByField(Person.name)).limit(10)
        for {
          l1 <- q.offset(0).docs.search.flatMap(_.list).map(_.map(_.name))
          l2 <- q.offset(10).docs.search.flatMap(_.list).map(_.map(_.name))
          l3 <- q.offset(20).docs.search.flatMap(_.list).map(_.map(_.name))
        } yield {
          l1 should be(List("Allan", "Brenda", "Charlie", "Diana", "Evan", "Fiona", "Greg", "Hanna", "Ian", "Jenna"))
          l2 should be(List("Kevin", "Mike", "Nancy", "Not Ruth", "Oscar", "Penny", "Quintin", "Sam", "Tori", "Uba"))
          l3 should be(List("Veronica", "Wyatt", "Xena", "Zoey"))
        }
      }
    }
    "sort by age and verify top results" in {
      db.people.transaction { transaction =>
        transaction.query.sort(Sort.ByField(Person.age).desc).limit(5).toList.map { people =>
          people.map(_.name) should be(List("Not Ruth", "Zoey", "Quintin", "Ian", "Sam"))
        }
      }
    }
    "sort by age double and verify top results" in {
      db.people.transaction { transaction =>
        transaction.query.sort(Sort.ByField(Person.ageDouble).desc).limit(5).toList.map { people =>
          people.map(_.name) should be(List("Not Ruth", "Zoey", "Quintin", "Ian", "Sam"))
        }
      }
    }
    "filter by created after" in {
      db.people.transaction { transaction =>
        transaction.query.filter(_.created >= start).toList.map { people =>
          people.map(_.name).toSet should be(Set("Brenda", "Charlie", "Diana", "Evan", "Fiona", "Greg", "Hanna", "Ian", "Jenna", "Kevin", "Mike", "Nancy", "Oscar", "Penny", "Quintin", "Sam", "Tori", "Uba", "Veronica", "Wyatt", "Xena", "Allan", "Zoey", "Not Ruth"))
        }
      }
    }
    "filter by list of friend ids" in {
      db.people.transaction { transaction =>
        val q = transaction.query.filter(_
          .builder
          .should(_.friends has fiona._id)
        )
        q.toList.map { list =>
          list.map(_.name).toSet should be(Set("Sam"))
        }
      }
    }
    "verify the correct number of records in the database" in {
      for {
        people <- db.people.t.count
        ageLinks <- db.ageLinks.t.count
        backingStore <- db.backingStore.t.count
      } yield {
        people should be(24)
        ageLinks should be(22)
        backingStore should be(3)
      }
    }
    "do a database backup" in {
      DatabaseBackup.archive(db.stores, new File(s"backups/$specName.zip")).map(_ should be(49))
    }
    "insert a lot more names" in {
      db.people.transaction { transaction =>
        val p = (1 to CreateRecords).toList.map { index =>
          Person(
            name = s"Unique Snowflake $index",
            age = if (index > 100) 0 else index,
            city = Some(City("Robotland")),
            nicknames = Set("robot", s"sf$index")
          )
        }
        transaction.insert(p).succeed
      }
    }
    "verify the correct number of people exist in the database" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(CreateRecords + 24))
      }
    }
    "verify id count matches total count" in {
      db.people.transaction { transaction =>
        transaction.query.countTotal(true).id.search.flatMap { results =>
          results.list.map { list =>
            results.total should be(Some(CreateRecords + 24))
            list.length should be(1_000)
          }
        }
      }
    }
    "verify total ids returned is correct" in {
      db.people.transaction { transaction =>
        transaction.query.countTotal(true).id.stream.toList.map { ids =>
          ids.length should be(CreateRecords + 24)
        }
      }
    }
    "verify the correct count in query total" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames.has("robot"))
          .sort(Sort.ByField(Person.age).descending)
          .limit(100)
          .countTotal(true)
          .docs
          .search
          .flatMap { results =>
            results.list.map { list =>
              list.length should be(100)
              results.total should be(Some(CreateRecords))
              results.remaining should be(Some(CreateRecords))
            }
          }
      }
    }
    "verify the correct count in query total with offset" in {
      db.people.transaction { transaction =>
        transaction.query
          .filter(_.nicknames has "robot")
          .limit(100)
          .offset(100)
          .countTotal(true)
          .docs
          .search
          .flatMap { results =>
            results.list.map { list =>
              list.length should be(100)
              results.total should be(Some(CreateRecords))
              results.remaining should be(Some(CreateRecords - 100))
            }
          }
      }
    }
    "truncate the collection" in {
      db.people.transaction { transaction =>
        transaction.truncate.map(_ should be(CreateRecords + 24))
      }
    }
    "verify the collection is empty" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(0))
      }
    }
    "restore from database backup" in {
      DatabaseRestore.archive(db, new File(s"backups/$specName.zip")).map(_ should be(49))
    }
    "verify the correct number of records exist" in {
      db.people.transaction { transaction =>
        transaction.count.map(_ should be(24))
      }
    }
    /*"insert an invalid record via JSON" in {
      db.people.transaction { transaction =>
        db.people.t.json.insert(List(
          obj(
            "name" -> "Invalid",
            "_id" -> "invalid-test"
          )
        ).iterator)
      }
    }
    "get the invalid JSON object" in {
      db.people.transaction { transaction =>
        val results = transaction.query.filter(_._id === Person.id("invalid-test")).search.json(_.fields)
        results.list.length should be(1)
        results.list.head should be(obj(
          "name" -> "Invalid",
          "_id" -> "invalid-test"
        ))
      }
    }*/
    "optimize an inefficient query" in {
      val query = Filter.Multi[Person](1, List(
        FilterClause(Filter.Multi[Person](1, List(
          FilterClause(Filter.Multi[Person](1, List(
            FilterClause(Filter.Contains(Person.name.name, "Adam"), Condition.Should, None),
            FilterClause(Filter.Contains(Person.name.name, "Zoe"), Condition.Should, None)
          )), Condition.Must, None)
        )), Condition.Must, None)
      ))
      val optimized = Filter.Multi[Person](1, List(
        FilterClause(Filter.Contains(Person.name.name, "Adam"), Condition.Should, None),
        FilterClause(Filter.Contains(Person.name.name, "Zoe"), Condition.Should, None)
      ))

      QueryOptimizer.optimize(query) should be(optimized)
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

    spec.features.foreach {
      case (key, value) =>
        put(key, value)
    }

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

    val people: Collection[Person, Person.type] = store(Person)
    val ageLinks: Collection[AgeLinks, AgeLinks.type] = store(AgeLinks)

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
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

  case class AgeLinks(age: Int, people: List[Id[Person]]) extends Document[AgeLinks] {
    lazy val _id: Id[AgeLinks] = AgeLinks.id(age)
  }

  object AgeLinks extends MaterializedBatchModel[AgeLinks, Person, Person.type] with JsonConversion[AgeLinks] {
    override implicit val rw: RW[AgeLinks] = RW.gen

    val age: F[Int] = field("age", _.age)
    val people: F[List[Id[Person]]] = field("people", _.people)

    override def materialStore: Store[Person, Person.type] = db.people

    def id(age: Int): Id[AgeLinks] = Id(age.toString)

    override protected def process(list: List[List[DocState[Person]]]): Task[Unit] = db.ageLinks.transaction { transaction =>
      Task {
        list.groupBy(_.head.doc.age).foreach {
          case (age, states) =>
            val firsts = states.map(_.head)
            val add = firsts.collect {
              case DocState.Added(doc) => doc._id
              case DocState.Modified(doc) => doc._id
            }
            val remove = firsts.collect {
              case DocState.Removed(doc) => doc._id
            }.toSet
            transaction.modify(AgeLinks.id(age)) { existing =>
              val current = existing.getOrElse(AgeLinks(age, Nil))
              val modified = current.copy(
                people = (current.people ::: add).filterNot(remove.contains)
              )
              if (modified.people.isEmpty) {
                Task.pure(None)
              } else {
                Task.pure(Some(modified))
              }
            }.sync()
        }
      }
    }
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): Task[Unit] = db.startTime.set(System.currentTimeMillis()).unit
  }
}
