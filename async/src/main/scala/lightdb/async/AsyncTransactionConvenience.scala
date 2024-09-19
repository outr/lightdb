package lightdb.async

import cats.effect.IO
import lightdb.doc.{Document, DocumentModel}
import lightdb._
import lightdb.field.Field._

case class AsyncTransactionConvenience[Doc <: Document[Doc], Model <: DocumentModel[Doc]](collection: AsyncCollection[Doc, Model]) {
  def insert(doc: Doc): IO[Doc] = collection.transaction { implicit transaction =>
    collection.insert(doc)
  }

  def insert(docs: Seq[Doc]): IO[Seq[Doc]] = collection.transaction { implicit transaction =>
    collection.insert(docs)
  }

  def upsert(doc: Doc): IO[Doc] = collection.transaction { implicit transaction =>
    collection.upsert(doc)
  }

  def upsert(docs: Seq[Doc]): IO[Seq[Doc]] = collection.transaction { implicit transaction =>
    collection.upsert(docs)
  }

  def get[V](f: Model => (UniqueIndex[Doc, V], V)): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(f)
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V)): IO[Doc] = collection.transaction { implicit transaction =>
    collection(f)
  }

  def get(id: Id[Doc]): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(id)
  }

  def apply(id: Id[Doc]): IO[Doc] = collection.transaction { implicit transaction =>
    collection(id)
  }

  def list: IO[List[Doc]] = collection.transaction { implicit transaction =>
    collection.stream.compile.toList
  }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => IO[Option[Doc]]): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.modify(id, lock, deleteOnNone)(f)
  }

  def getOrCreate(id: Id[Doc], create: => IO[Doc], lock: Boolean = true): IO[Doc] = collection.transaction { implicit transaction =>
    collection.getOrCreate(id, create, lock)
  }

  def delete[V](f: Model => (UniqueIndex[Doc, V], V)): IO[Boolean] = collection.transaction { implicit transaction =>
    collection.delete(f)
  }

  def delete(id: Id[Doc])(implicit ev: Model <:< DocumentModel[_]): IO[Boolean] = collection.transaction { implicit transaction =>
    collection.delete(id)
  }

  def count: IO[Int] = collection.transaction { implicit transaction =>
    collection.count
  }

  def truncate(): IO[Int] = collection.transaction { implicit transaction =>
    collection.truncate()
  }
}
