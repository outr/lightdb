package lightdb.store

import cats.effect.IO
import lightdb.Id
import lightdb.document.Document
import lightdb.error.IdNotFoundException
import lightdb.transaction.Transaction
import lightdb.util.Initializable

trait Store[D <: Document[D]] extends Initializable {
  type Serialized

  protected def serialize(doc: D): IO[Serialized]
  protected def deserialize(serialized: Serialized): IO[D]

  protected def setSerialized(id: Id[D], serialized: Serialized, transaction: Transaction[D]): IO[Boolean]
  protected def getSerialized(id: Id[D], transaction: Transaction[D]): IO[Option[Serialized]]
  protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, Serialized]

  def apply(id: Id[D])(implicit transaction: Transaction[D]): IO[D] = get(id)
    .map {
      case Some(doc) => doc
      case None => throw IdNotFoundException(id)
    }

  def get(id: Id[D])(implicit transaction: Transaction[D]): IO[Option[D]] = getSerialized(id, transaction)
    .flatMap {
      case Some(serialized) => deserialize(serialized).map(Some.apply)
      case None => IO.pure(None)
    }

  def set(doc: D)(implicit transaction: Transaction[D]): IO[Boolean] = serialize(doc)
    .flatMap { serialized =>
      setSerialized(doc._id, serialized, transaction)
    }

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = idStream
    .evalMap(get)
    .unNone

  def count(implicit transaction: Transaction[D]): IO[Int]
  def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]]
  def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean]
  def truncate()(implicit transaction: Transaction[D]): IO[Int]

  def dispose(): IO[Unit]
}