package lightdb.collection

import cats.effect.IO
import cats.implicits.toTraverseOps
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

  def get(id: Id[D]): IO[Option[D]] = {
    db.verifyInitialized()
    data.get(id)
  }

  def fromArray(array: Array[Byte]): D = data.fromArray(array)

  def apply(id: Id[D]): IO[D] = data(id)

  def put(value: D): IO[D] = {
    db.verifyInitialized()
    for {
      _ <- data.put(value._id, value)
      _ <- indexer.put(value)
    } yield {
      value
    }
  }

  def putAll(values: Seq[D]): IO[Seq[D]] = values.map(put).sequence

  def putStream(stream: fs2.Stream[IO, D]): IO[Int] = stream.evalMap(put).compile.count.map(_.toInt)

  def all(chunkSize: Int = 512, maxConcurrent: Int = 16): fs2.Stream[IO, D] = {
    db.verifyInitialized()
    store
      .all[D](chunkSize)
      .mapAsync(maxConcurrent)(t => IO(dataManager.fromArray(t.data)))
  }

  def modify(id: Id[D])(f: Option[D] => Option[D]): IO[Option[D]] = {
    db.verifyInitialized()
    for {
      result <- data.modify(id)(f)
      _ <- result.map(indexer.put).getOrElse(IO.unit)
    } yield {
      result
    }
  }

  def delete(id: Id[D]): IO[Unit] = {
    db.verifyInitialized()
    for {
      _ <- data.delete(id)
      _ <- indexer.delete(id)
    } yield {
      ()
    }
  }

  def deleteAll(ids: Seq[Id[D]]): IO[Unit] = ids.map(delete).sequence.map(_ => ())

  def deleteStream(stream: fs2.Stream[IO, Id[D]]): IO[Int] = stream.evalMap(delete).compile.count.map(_.toInt)

  def commit(): IO[Unit] = {
    db.verifyInitialized()
    for {
      _ <- data.commit()
      _ <- indexer.commit()
    } yield {
      ()
    }
  }

  def truncate(): IO[Unit] = {
    db.verifyInitialized()
    for {
      _ <- store.truncate()
      _ <- indexer.truncate()
    } yield {
      ()
    }
  }

  def dispose(): IO[Unit] = if (db.initialized) {
    for {
      _ <- store.dispose()
      _ <- indexer.dispose()
    } yield ()
  } else {
    IO.unit
  }

  protected lazy val data: CollectionData[D] = CollectionData(this)
}
