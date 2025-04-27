package lightdb.store

import fabric._
import fabric.rw._
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.store.split.SplitCollection
import rapid.{Forge, Task}

case class Transactionless[Doc <: Document[Doc], +Model <: DocumentModel[Doc]](store: Store[Doc, Model]) {
  /**
   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
   */
  def insert(doc: Doc): Task[Doc] = store.transaction { implicit transaction =>
    store.insert(doc)
  }

  def upsert(doc: Doc): Task[Doc] = store.transaction { implicit transaction =>
    store.upsert(doc)
  }

  def insert(docs: Seq[Doc]): Task[Seq[Doc]] = store.transaction { implicit transaction =>
    store.insert(docs)
  }

  def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = store.transaction { implicit transaction =>
    store.upsert(docs)
  }

  def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = store.transaction { implicit transaction =>
    store.get(f)
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = store.transaction { implicit transaction =>
    store(f)
  }

  def get(id: Id[Doc]): Task[Option[Doc]] = store.transaction { implicit transaction =>
    store.get(id)
  }

  def getAll(ids: Seq[Id[Doc]]): Task[List[Doc]] = store.transaction { implicit transaction =>
    store.getAll(ids).toList
  }

  def apply(id: Id[Doc]): Task[Doc] = store.transaction { implicit transaction =>
    store(id)
  }

  object json {
    def insert(stream: rapid.Stream[Json],
               disableSearchUpdates: Boolean): Task[Int] = store.transaction { implicit transaction =>
      if (disableSearchUpdates) {
        transaction.put(SplitCollection.NoSearchUpdates, true)
      }
      stream
        .map(_.as[Doc](store.model.rw))
        .evalMap(store.insert)
        .count
    }

    def stream[Return](f: rapid.Stream[Json] => Task[Return]): Task[Return] = store.transaction { implicit transaction =>
      f(store.jsonStream)
    }
  }

  def list(): Task[List[Doc]] = store.transaction { implicit transaction =>
    store.list()
  }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = store.transaction { implicit transaction =>
    store.modify(id, lock, deleteOnNone)(f)
  }

  def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = store.transaction { implicit transaction =>
    store.delete(f)
  }

  def delete(id: Id[Doc]): Task[Boolean] = store.transaction { implicit transaction =>
    store.delete(id)
  }

  def count: Task[Int] = store.transaction { implicit transaction =>
    store.count
  }

  def truncate(): Task[Int] = store.transaction { implicit transaction =>
    store.truncate()
  }
}
