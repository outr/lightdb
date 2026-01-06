package lightdb.store

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.split.{SearchUpdateHandler, SplitCollectionTransaction}
import rapid.{Forge, Task}

/**
 * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
 */
case class Transactionless[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model]) {
  def insert(doc: Doc): Task[Doc] = store.transaction(_.insert(doc))

  def upsert(doc: Doc): Task[Doc] = store.transaction(_.upsert(doc))

  def insert(docs: Seq[Doc]): Task[Seq[Doc]] = store.transaction(_.insert(docs))

  def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = store.transaction(_.upsert(docs))

  def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = store.transaction(_.get(f))

  def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = store.transaction(_(f))

  def get(id: Id[Doc]): Task[Option[Doc]] = store.transaction(_.get(id))

  def getAll(ids: Seq[Id[Doc]]): Task[List[Doc]] = store.transaction(_.getAll(ids).toList)

  def apply(id: Id[Doc]): Task[Doc] = store.transaction(_(id))

  object json {
    def insert(stream: rapid.Stream[Json],
               disableSearchUpdates: Boolean): Task[Int] = store.transaction { transaction =>
      if (disableSearchUpdates) {
        transaction match {
          case t: SplitCollectionTransaction[_, _, _, _] => t.disableSearchUpdate()
          case _ => // Ignore others
        }
      }
      transaction.insert(stream.map(_.as[Doc](store.model.rw)))
    }

    def stream[Return](f: rapid.Stream[Json] => Task[Return]): Task[Return] = store.transaction(t => f(t.jsonStream))
  }

  def list: Task[List[Doc]] = store.transaction(_.list)

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] =
    store.transaction(_.modify(id, lock, deleteOnNone)(f))

  def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = store.transaction(_.delete(f))

  def delete(id: Id[Doc]): Task[Boolean] = store.transaction(_.delete(id))

  def count: Task[Int] = store.transaction(_.count)

  def truncate: Task[Int] = store.transaction(_.truncate)
}
