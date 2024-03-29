package lightdb.store
import cats.effect.IO
import lightdb.collection.Collection
import lightdb.{Document, Id}

object NullStore extends ObjectStore {
  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO.pure(None)

  override def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]] = IO.pure(value)

  override def delete[T](id: Id[T]): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.unit

  override def count(): IO[Int] = IO.pure(0)

  override def all[T](chunkSize: Int): fs2.Stream[IO, ObjectData[T]] = fs2.Stream.empty

  override def commit(): IO[Unit] = IO.unit

  override def truncate(): IO[Unit] = IO.unit
}

trait NullStoreSupport extends ObjectStoreSupport {
  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = NullStore
}