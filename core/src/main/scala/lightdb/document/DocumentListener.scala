package lightdb.document

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.transaction.Transaction
import moduload.Priority

trait DocumentListener[D <: Document[D]] {
  def priority: Priority = Priority.Normal

  def init(collection: Collection[D, _]): IO[Unit] = IO.unit

  def transactionStart(transaction: Transaction[D]): IO[Unit] = IO.unit

  def transactionEnd(transaction: Transaction[D]): IO[Unit] = IO.unit

  def preSet(doc: D, transaction: Transaction[D]): IO[Option[D]] = IO.pure(Some(doc))

  def postSet(doc: D, transaction: Transaction[D]): IO[Unit] = IO.unit

  def commit(transaction: Transaction[D]): IO[Unit] = IO.unit

  def rollback(transaction: Transaction[D]): IO[Unit] = IO.unit

  def preDelete(doc: D, transaction: Transaction[D]): IO[Option[D]] = IO.pure(Some(doc))

  def postDelete(doc: D, transaction: Transaction[D]): IO[Unit] = IO.unit

  def truncate(transaction: Transaction[D]): IO[Unit] = IO.unit

  def dispose(): IO[Unit] = IO.unit
}