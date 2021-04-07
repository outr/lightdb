package lightdb

import cats.effect.IO
import fabric.rw._
import lightdb.data.{DataManager, JsonDataManager}
import lightdb.store.HaloStore

case class Collection[T](db: LightDB, mapping: ObjectMapping[T]) {
  protected def dataManager: DataManager[T] = mapping.dataManager
  protected def indexer: Indexer[T] = mapping.indexer

  def get(id: Id[T]): IO[Option[T]] = data.get(id)

  def apply(id: Id[T]): IO[T] = data(id)

  def put(id: Id[T], value: T): IO[T] = for {
    _ <- data.put(id, value)
    _ <- indexer.put(id, value)
  } yield {
    value
  }

  def modify(id: Id[T])(f: Option[T] => Option[T]): IO[Option[T]] = for {
    result <- data.modify(id)(f)
    _ <- result.map(indexer.put(id, _)).getOrElse(IO.unit)
  } yield {
    result
  }

  def delete(id: Id[T]): IO[Unit] = for {
    _ <- data.delete(id)
    _ <- indexer.delete(id)
  } yield {
    ()
  }

  protected lazy val data: CollectionData[T] = CollectionData(this)
}

trait Indexer[T] {
  def put(id: Id[T], value: T): IO[T]
  def delete(id: Id[T]): IO[Unit]
}

case class CollectionData[T](collection: Collection[T]) {
  protected def db: LightDB = collection.db
  protected def dataManager: DataManager[T] = collection.mapping.dataManager

  def get(id: Id[T]): IO[Option[T]] = db.store.get(id).map(_.map(dataManager.fromArray))

  def apply(id: Id[T]): IO[T] = get(id).map(_.getOrElse(throw new RuntimeException(s"Not found by id: $id")))

  def put(id: Id[T], value: T): IO[T] = db.store.put(id, dataManager.toArray(value)).map(_ => value)

  def modify(id: Id[T])(f: Option[T] => Option[T]): IO[Option[T]] = {
    var result: Option[T] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }

  def delete(id: Id[T]): IO[Unit] = db.store.delete(id)
}

trait Field {
  def name: String
}

trait IndexedField[F] extends Field {

}

trait ObjectMapping[T] {
  def fields: List[Field]
  def dataManager: DataManager[T]
  def indexer: Indexer[T]
}

object Test extends LightDB(new HaloStore) {
  val people: Collection[Person] = Collection(this, Person)

  case class Person(name: String, age: Int)

  object Person extends ObjectMapping[Person] {
    implicit val rw: ReaderWriter[Person] = ccRW

    lazy val dataManager: DataManager[Person] = new JsonDataManager[Person]
    lazy val indexer: Indexer[Person] = ???

    lazy val name: IndexedField[String] = ???
    lazy val age: IndexedField[Int] = ???

    override lazy val fields: List[Field] = List(name, age)
  }
}