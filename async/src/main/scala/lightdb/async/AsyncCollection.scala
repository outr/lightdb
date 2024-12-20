package lightdb.async

import lightdb._
import lightdb.field.Field._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction
import rapid.Task

import scala.util.{Failure, Success}

case class AsyncCollection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](underlying: Collection[Doc, Model]) extends AnyVal {
  def transaction[Return](f: Transaction[Doc] => Task[Return]): Task[Return] = {
    val transaction = underlying.transaction.create()
    f(transaction).guarantee(Task {
      underlying.transaction.release(transaction)
    })
  }

  /**
   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
   */
  def t: AsyncTransactionConvenience[Doc, Model] = AsyncTransactionConvenience(this)

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task(underlying.insert(doc))

  def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = Task(underlying.insert(docs))

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task(underlying.upsert(doc))

  def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = Task(underlying.upsert(docs))

  def get[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Option[Doc]] =
    Task(underlying.get(f))

  def apply[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Doc] =
    Task(underlying(f))

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Option[Doc]] =
    Task(underlying.get(id))

  def getAll(ids: Seq[Id[Doc]])(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.fromIterator(Task(underlying.getAll(ids)))

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Doc] =
    Task(underlying(id))

  def withLock(id: Id[Doc], doc: Task[Option[Doc]], establishLock: Boolean = true)
           (f: Option[Doc] => Task[Option[Doc]]): Task[Option[Doc]] = if (establishLock) {
    doc.map(d => if (establishLock) underlying.lock.acquire(id, d) else d).flatMap { existing =>
      f(existing)
        .attempt
        .flatMap {
          case Success(modified) =>
            if (establishLock) underlying.lock.release(id, modified)
            Task.pure(modified)
          case Failure(err) =>
            if (establishLock) underlying.lock.release(id, existing)
            Task.error(err)
        }
    }
  } else {
    doc.flatMap(f)
  }

  def modify(id: Id[Doc], establishLock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Task[Option[Doc]])
            (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = withLock(id, get(id), establishLock) { existing =>
    f(existing).flatMap {
      case Some(doc) => upsert(doc).map(doc => Some(doc))
      case None if deleteOnNone => delete(id).map(_ => None)
      case None => Task.pure(None)
    }
  }

  def getOrCreate(id: Id[Doc], create: => Task[Doc], lock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): Task[Doc] = modify(id, establishLock = lock) {
    case Some(doc) => Task.pure(Some(doc))
    case None => create.map(Some.apply)
  }.map(_.get)

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(underlying.delete(f))

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Task[Boolean] =
    Task(underlying.delete(id))

  def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(underlying.count)

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.fromIterator(Task(underlying.iterator))

  def list(implicit transaction: Transaction[Doc]): Task[List[Doc]] = stream.toList

  def query: AsyncQuery[Doc, Model] = AsyncQuery(this)

  def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task(underlying.truncate())

  def reIndex(): Task[Boolean] = Task(underlying.reIndex())

  def dispose(): Task[Unit] = Task(underlying.dispose())
}