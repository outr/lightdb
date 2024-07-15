//package benchmark.bench.impl
//
//import benchmark.bench.Bench
//import cats.effect.IO
//import cats.effect.unsafe.implicits.global
//import com.outr.arango.collection.DocumentCollection
//import fabric.rw.RW
//import lightdb.halo.HaloDBSupport
//import lightdb.lucene.LuceneSupport
//import lightdb.model.{AbstractCollection, DocumentModel}
//import lightdb.upgrade.DatabaseUpgrade
//import lightdb.{Document, Id, LightDB}
//
//import java.nio.file.{Path, Paths}
//
//object LightDB011Bench extends Bench {
//  override def name: String = "LightDB 0.11"
//
//  override def init(): Unit = DB.init().unsafeRunSync()
//
//  override protected def insertRecords(iterator: Iterator[LightDB011Bench.P]): Unit = {
//    fs2.Stream.fromBlockingIterator[IO](iterator.map { p =>
//      val person = Person(
//        name = p.name,
//        age = p.age,
//        _id = Person.id(p.id)
//      )
//      DB.people.set(person).unsafeRunSync()
//    }, 512).compile.drain.flatMap(_ => DB.people.commit()).unsafeRunSync()
//  }
//
//  override protected def streamRecords(f: Iterator[LightDB011Bench.P] => Unit): Unit = {
//    f(DB.people.stream.compile.toList.unsafeRunSync().iterator.map(person => P(
//      name = person.name,
//      age = person.age,
//      id = person._id.value
//    )))
//  }
//
//  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = {
//    ageIterator.foreach { age =>
//      val list = Person.query.filter(Person.age === age).toList.unsafeRunSync()
//      val person = list.head
//      if (person.age != age) {
//        scribe.warn(s"${person.age} was not $age")
//      }
//      if (list.size > 1) {
//        scribe.warn(s"More than one result for $age")
//      }
//    }
//  }
//
//  override protected def searchAllRecords(f: Iterator[LightDB011Bench.P] => Unit): Unit = {
//    Person.withSearchContext { implicit context =>
//      Person.query.stream.compile.toList.map { list =>
//        f(list.iterator.map { person =>
//          P(
//            name = person.name,
//            age = person.age,
//            id = person._id.value
//          )
//        })
//      }
//    }.unsafeRunSync()
//  }
//
//  override def size(): Long = 0L
//
//  override def dispose(): Unit = DB.dispose().unsafeRunSync()
//
//  object DB extends LightDB with HaloDBSupport {
//    override def directory: Path = Paths.get("db/ldb011")
//
//    val people: AbstractCollection[Person] = collection("people", Person)
//
//    override def userCollections: List[AbstractCollection[_]] = List(people)
//
//    override def upgrades: List[DatabaseUpgrade] = Nil
//  }
//
//  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]
//  object Person extends DocumentModel[Person] with LuceneSupport[Person] {
//    implicit val rw: RW[Person] = RW.gen
//
//    val age: I[Int] = index.one("age", _.age)
//  }
//}
