package lightdb.collection

import cats.effect.IO
import lightdb.data.DataManager
import lightdb.index.Indexer
import lightdb.query.Query
import lightdb.store.ObjectStore
import lightdb.{Document, Id, LightDB, ObjectMapping}

case class Collection[D <: Document[D]](db: LightDB, mapping: ObjectMapping[D], collectionName: String) {
  protected def dataManager: DataManager[D] = mapping.dataManager

  lazy val store: ObjectStore = db.store[D](this)
  lazy val indexer: Indexer[D] = db.indexer(this)
  lazy val query: Query[D] = Query[D](this)

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

  def commit(): IO[Unit] = for {
    _ <- data.commit()
    _ <- indexer.commit()
  } yield {
    ()
  }

  def truncate(): IO[Unit] = for {
    _ <- store.truncate()
    _ <- indexer.truncate()
  } yield {
    ()
  }

  def dispose(): IO[Unit] = {
    indexer.dispose()
  }

  protected lazy val data: CollectionData[D] = CollectionData(this)
}
