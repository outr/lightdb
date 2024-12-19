package lightdb.async

import lightdb.doc.{Document, DocumentModel}
import lightdb._
import lightdb.field.Field._
import rapid.Task

case class AsyncTransactionConvenience[Doc <: Document[Doc], Model <: DocumentModel[Doc]](collection: AsyncCollection[Doc, Model]) {
  def insert(doc: Doc): Task[Doc] = collection.transaction { implicit transaction =>
    collection.insert(doc)
  }

  def insert(docs: Seq[Doc]): Task[Seq[Doc]] = collection.transaction { implicit transaction =>
    collection.insert(docs)
  }

  def upsert(doc: Doc): Task[Doc] = collection.transaction { implicit transaction =>
    collection.upsert(doc)
  }

  def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = collection.transaction { implicit transaction =>
    collection.upsert(docs)
  }

  def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(f)
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = collection.transaction { implicit transaction =>
    collection(f)
  }

  def get(id: Id[Doc]): Task[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(id)
  }

  def apply(id: Id[Doc]): Task[Doc] = collection.transaction { implicit transaction =>
    collection(id)
  }

  def list: Task[List[Doc]] = collection.transaction { implicit transaction =>
    collection.stream.toList
  }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Task[Option[Doc]]): Task[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.modify(id, lock, deleteOnNone)(f)
  }

  def getOrCreate(id: Id[Doc], create: => Task[Doc], lock: Boolean = true): Task[Doc] = collection.transaction { implicit transaction =>
    collection.getOrCreate(id, create, lock)
  }

  def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = collection.transaction { implicit transaction =>
    collection.delete(f)
  }

  def delete(id: Id[Doc])(implicit ev: Model <:< DocumentModel[_]): Task[Boolean] = collection.transaction { implicit transaction =>
    collection.delete(id)
  }

  def count: Task[Int] = collection.transaction { implicit transaction =>
    collection.count
  }

  def truncate(): Task[Int] = collection.transaction { implicit transaction =>
    collection.truncate()
  }
}
