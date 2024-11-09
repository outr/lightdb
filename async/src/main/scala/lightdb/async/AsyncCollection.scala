package lightdb.async

import cats.effect.IO
import lightdb._
import lightdb.field.Field._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction

case class AsyncCollection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](underlying: Collection[Doc, Model]) extends AnyVal {
  def transaction[Return](f: Transaction[Doc] => IO[Return]): IO[Return] = {
    val transaction = underlying.transaction.create()
    f(transaction).guarantee(IO {
      underlying.transaction.release(transaction)
    })
  }

  /**
   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
   */
  def t: AsyncTransactionConvenience[Doc, Model] = AsyncTransactionConvenience(this)

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): IO[Doc] = IO.blocking(underlying.insert(doc))

  def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): IO[Seq[Doc]] = IO.blocking(underlying.insert(docs))

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): IO[Doc] = IO.blocking(underlying.upsert(doc))

  def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): IO[Seq[Doc]] = IO.blocking(underlying.upsert(docs))

  def get[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Option[Doc]] =
    IO.blocking(underlying.get(f))

  def apply[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Doc] =
    IO.blocking(underlying(f))

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): IO[Option[Doc]] =
    IO.blocking(underlying.get(id))

  def getAll(ids: Seq[Id[Doc]])(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Doc] =
    fs2.Stream.fromBlockingIterator[IO](underlying.getAll(ids), 512)

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): IO[Doc] =
    IO.blocking(underlying(id))

  def withLock(id: Id[Doc], doc: IO[Option[Doc]], establishLock: Boolean = true)
           (f: Option[Doc] => IO[Option[Doc]]): IO[Option[Doc]] = if (establishLock) {
    doc.map(d => if (establishLock) underlying.lock.acquire(id, d) else d).flatMap { existing =>
      f(existing)
        .attempt
        .flatMap {
          case Left(err) =>
            if (establishLock) underlying.lock.release(id, existing)
            IO.raiseError(err)
          case Right(modified) =>
            if (establishLock) underlying.lock.release(id, modified)
            IO.pure(modified)
        }
    }
  } else {
    doc.flatMap(f)
  }

  def modify(id: Id[Doc], establishLock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => IO[Option[Doc]])
            (implicit transaction: Transaction[Doc]): IO[Option[Doc]] = withLock(id, get(id), establishLock) { existing =>
    f(existing).flatMap {
      case Some(doc) => upsert(doc).map(doc => Some(doc))
      case None if deleteOnNone => delete(id).map(_ => None)
      case None => IO.pure(None)
    }
  }

  def getOrCreate(id: Id[Doc], create: => IO[Doc], lock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): IO[Doc] = modify(id, establishLock = lock) {
    case Some(doc) => IO.pure(Some(doc))
    case None => create.map(Some.apply)
  }.map(_.get)

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Boolean] =
    IO.blocking(underlying.delete(f))

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): IO[Boolean] =
    IO.blocking(underlying.delete(id))

  def count(implicit transaction: Transaction[Doc]): IO[Int] = IO.blocking(underlying.count)

  def stream(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Doc] = fs2.Stream
    .fromBlockingIterator[IO](underlying.iterator, 512)

  def list(implicit transaction: Transaction[Doc]): IO[List[Doc]] = stream.compile.toList

  def query: AsyncQuery[Doc, Model] = AsyncQuery(this)

  def truncate()(implicit transaction: Transaction[Doc]): IO[Int] = IO.blocking(underlying.truncate())

  def reIndex(): IO[Boolean] = IO.blocking(underlying.reIndex())

  def dispose(): IO[Unit] = IO.blocking(underlying.dispose())
}