package lightdb.collection

import cats.effect.IO
import lightdb.data.DataManager
import lightdb.index.Indexer
import lightdb.{Document, Id, LightDB, ObjectMapping}

case class Collection[D <: Document[D]](db: LightDB, mapping: ObjectMapping[D]) {
  protected def dataManager: DataManager[D] = mapping.dataManager

  protected def indexer: Indexer = db.indexer

  def get(id: Id[D]): IO[Option[D]] = data.get(id)

  def apply(id: Id[D]): IO[D] = data(id)

  def put(value: D): IO[D] = for {
    _ <- data.put(value._id, value)
    _ <- indexer.put(value, mapping)
  } yield {
    value
  }

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = for {
    result <- data.modify(id)(f)
    _ <- result.map(indexer.put(_, mapping)).getOrElse(IO.unit)
  } yield {
    result
  }

  def delete(id: Id[D]): IO[Unit] = for {
    _ <- data.delete(id)
    _ <- indexer.delete(id, mapping)
  } yield {
    ()
  }

  def flush(): IO[Unit] = for {
    _ <- data.flush()
    _ <- indexer.commit(mapping)
  } yield {
    ()
  }

  def dispose(): IO[Unit] = {
    indexer.dispose()
  }

  protected lazy val data: CollectionData[D] = CollectionData(this)
}
