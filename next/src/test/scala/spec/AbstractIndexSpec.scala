package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.Indexer
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

abstract class AbstractIndexSpec extends AsyncWordSpec with AsyncIOSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init().map(b => b should be(true))
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  protected def storeManager: StoreManager
  protected def indexer: Indexer[Person]

  object DB extends LightDB {
    val people: Collection[Person] = collection("people", Person)

    override def storeManager: StoreManager = spec.storeManager
  }

  case class Person(name: String,
                    age: Int,
                    tags: Set[String],
                    point: GeoPoint,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] {
    implicit val rw: RW[Person] = RW.gen

    val index: Indexer[Person] = spec.indexer

    val name: I[String] = index.one("name", _.name)
    val age: I[Int] = index.one("age", _.age)
    val tag: I[String] = index("tag", _.tags.toList)
    val point: I[GeoPoint] = index.one("point", _.point, sorted = true)
    val search: I[String] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }
}
