package lightdb.collection

import cats.effect.IO
import lightdb.data.DataManager
import lightdb.{Document, Id, LightDB}

case class CollectionData[D <: Document[D]](collection: Collection[D]) {
  protected def db: LightDB = collection.db

  protected def dataManager: DataManager[D] = collection.mapping.dataManager

  def fromArray(array: Array[Byte]): D = dataManager.fromArray(array)

  def get(id: Id[D]): IO[Option[D]] = collection.store.get(id).map(_.map(fromArray))

  def apply(id: Id[D]): IO[D] = get(id).map(_.getOrElse(throw new RuntimeException(s"Not found by id: $id")))

  def put(id: Id[D], value: D): IO[D] = collection.store.put(id, dataManager.toArray(value)).map(_ => value)

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = {
    var result: Option[D] = None
    collection.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }

  def delete(id: Id[D]): IO[Unit] = collection.store.delete(id)

  def commit(): IO[Unit] = collection.store.commit()
}