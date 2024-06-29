package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import fabric.rw.RW
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, IndexerManager}
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.Unique
import lightdb.{Id, LightDB}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Path
import scala.collection.parallel.CollectionConverters._

case class LightDBBench(sm: StoreManager, im: IndexerManager) extends Bench {
  override def init(): Unit = {
    val dbDir = new File("db")
    FileUtils.deleteDirectory(dbDir)
    dbDir.mkdirs()

    scribe.info("DB init...")
    DB.init()
    scribe.info("Initialized!")
  }

  override protected def insertRecords(status: StatusCallback): Unit = DB.people.transaction { implicit transaction =>
    (0 until RecordCount)
      .foreach { index =>
        DB.people.set(Person(
          name = Unique(),
          age = index
        ))
        status.progress.set(index + 1)
      }
  }

  override protected def streamRecords(status: StatusCallback): Unit = DB.people.transaction { implicit transaction =>
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        val count = DB.people.iterator.size
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress.set(iteration + 1)
      }
  }

  override protected def searchEachRecord(status: StatusCallback): Unit = DB.people.transaction { implicit transaction =>
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        (0 until RecordCount)
          .par
          .foreach { index =>
            val list = DB.people.query.filter(_.age === index).search.docs.list
            if (list.size != 1) {
              scribe.warn(s"Unable to find age = $index")
            }
            if (list.head.age != index) {
              scribe.warn(s"${list.head.age} was not $index")
            }
            status.progress.set((iteration + 1) * (index + 1))
          }
      }
  }

  override protected def searchAllRecords(status: StatusCallback): Unit = DB.people.transaction { implicit transaction =>
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        val count = DB.people.query.search.docs.iterator.foldLeft(0)((count, _) => count + 1)
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress.set(iteration + 1)
      }
  }

  override def dispose(): Unit = DB.dispose()

  object DB extends LightDB {
    override lazy val directory: Option[Path] = Some(Path.of(s"db/bench"))

    val people: IndexedCollection[Person, Person.type] = collection("people", Person, im.create[Person, Person.type]())

    override def storeManager: StoreManager = sm

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name, store = true)
    val age: I[Int] = index.one("age", _.age, store = true)
  }
}
