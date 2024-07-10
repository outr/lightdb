package lightdb.async

import cats.effect.IO
import lightdb.{Field, Id}
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

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): IO[Doc] = IO.blocking(underlying.set(doc))

  def set(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): IO[Seq[Doc]] = IO.blocking(underlying.set(docs))

  def get[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Option[Doc]] =
    IO.blocking(underlying.get(f))

  def apply[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Doc] =
    IO.blocking(underlying(f))

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): IO[Option[Doc]] =
    IO.blocking(underlying.get(id))

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): IO[Doc] =
    IO.blocking(underlying(id))

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): IO[Option[Doc]] =
    IO.blocking(underlying.modify(id, lock, deleteOnNone)(f))

  def delete[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): IO[Boolean] =
    IO.blocking(underlying.delete(f))

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): IO[Boolean] =
    IO.blocking(underlying.delete(id))

  def count(implicit transaction: Transaction[Doc]): IO[Int] = IO.blocking(underlying.count)

  def stream(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Doc] = fs2.Stream
    .fromBlockingIterator[IO](underlying.iterator, 512)

  def query: AsyncQuery[Doc, Model] = AsyncQuery(underlying)

  def truncate()(implicit transaction: Transaction[Doc]): IO[Int] = IO.blocking(underlying.truncate())

  def dispose(): IO[Unit] = IO.blocking(underlying.dispose())
}