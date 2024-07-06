package lightdb.async

import cats.effect.IO
import lightdb.doc.{DocModel, DocumentModel}
import lightdb.{Field, Id}

case class AsyncTransactionConvenience[Doc, Model <: DocModel[Doc]](collection: AsyncCollection[Doc, Model]) {
  def set(doc: Doc): IO[Doc] = collection.transaction { implicit transaction =>
    collection.set(doc)
  }

  def set(docs: Seq[Doc]): IO[Seq[Doc]] = collection.transaction { implicit transaction =>
    collection.set(docs)
  }

  def get[V](f: Model => (Field.Unique[Doc, V], V)): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(f)
  }

  def apply[V](f: Model => (Field.Unique[Doc, V], V)): IO[Doc] = collection.transaction { implicit transaction =>
    collection(f)
  }

  def get(id: Id[Doc])(implicit ev: Model <:< DocumentModel[_]): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.get(id)
  }

  def apply(id: Id[Doc])(implicit ev: Model <:< DocumentModel[_]): IO[Doc] = collection.transaction { implicit transaction =>
    collection(id)
  }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc]): IO[Option[Doc]] = collection.transaction { implicit transaction =>
    collection.modify(id, lock, deleteOnNone)(f)
  }

  def delete[V](f: Model => (Field.Unique[Doc, V], V)): IO[Boolean] = collection.transaction { implicit transaction =>
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
