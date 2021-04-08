package lightdb

import cats.effect.{ExitCode, IO, IOApp}
import fabric.rw._
import lightdb.data.{DataManager, JsonDataManager}
import lightdb.field.Field
import lightdb.index.{Indexer, LuceneIndexer}
import lightdb.store.HaloStore
import org.apache.lucene.store.FSDirectory

import java.nio.file.Paths

case class Collection[D <: Document[D]](db: LightDB, mapping: ObjectMapping[D]) {
  protected def dataManager: DataManager[D] = mapping.dataManager
  protected def indexer: Indexer[D] = mapping.indexer

  def get(id: Id[D]): IO[Option[D]] = data.get(id)

  def apply(id: Id[D]): IO[D] = data(id)

  def put(value: D): IO[D] = for {
    _ <- data.put(value._id, value)
    _ <- indexer.put(value)
  } yield {
    value
  }

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = for {
    result <- data.modify(id)(f)
    _ <- result.map(indexer.put).getOrElse(IO.unit)
  } yield {
    result
  }

  def delete(id: Id[D]): IO[Unit] = for {
    _ <- data.delete(id)
    _ <- indexer.delete(id)
  } yield {
    ()
  }

  def dispose(): IO[Unit] = {
    indexer.dispose()
  }

  protected lazy val data: CollectionData[D] = CollectionData(this)
}

case class CollectionData[D <: Document[D]](collection: Collection[D]) {
  protected def db: LightDB = collection.db
  protected def dataManager: DataManager[D] = collection.mapping.dataManager

  def get(id: Id[D]): IO[Option[D]] = db.store.get(id).map(_.map(dataManager.fromArray))

  def apply(id: Id[D]): IO[D] = get(id).map(_.getOrElse(throw new RuntimeException(s"Not found by id: $id")))

  def put(id: Id[D], value: D): IO[D] = db.store.put(id, dataManager.toArray(value)).map(_ => value)

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = {
    var result: Option[D] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }

  def delete(id: Id[D]): IO[Unit] = db.store.delete(id)
}

object Test extends LightDB(new HaloStore) with IOApp {
  val people: Collection[Person] = Collection(this, Person)

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends ObjectMapping[Person] {
    implicit val rw: ReaderWriter[Person] = ccRW

    override def collectionName: String = "people"

    lazy val dataManager: DataManager[Person] = new JsonDataManager[Person]
    lazy val indexer: LuceneIndexer[Person] = new LuceneIndexer[Person](this, FSDirectory.open(Paths.get("db/index")))

    lazy val name: Field[Person, String] = field[String]("name", indexer.string(_.name))
    lazy val age: Field[Person, Int] = field[Int]("age", indexer.int(_.age))

    override lazy val fields: List[Field[Person, _]] = List(name, age)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val id1 = Id[Person]("john")
    val id2 = Id[Person]("jane")

    val p1 = Person("John Doe", 21, id1)
    val p2 = Person("Jane Doe", 19, id2)

    for {
      _ <- people.put(p1)
      _ <- people.put(p2)
      g1 <- people.get(id1)
      _ = assert(g1.contains(p1), s"$g1 did not contain $p1")
      g2 <- people.get(id2)
      _ = assert(g2.contains(p2), s"$g2 did not contain $p2")
      // TODO: query from the index
      _ <- people.mapping.indexer.asInstanceOf[LuceneIndexer[Person]].test()
      _ <- dispose()
    } yield {
      ExitCode.Success
    }
  }
}