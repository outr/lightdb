package lightdb.trigger

import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.Transaction
import rapid.Task

trait BasicCollectionTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends CollectionTrigger[Doc] {
  def collection: Collection[Doc, Model]

  protected def adding(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  protected def modifying(oldDoc: Doc, newDoc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  protected def removing(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  override final def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = adding(doc)

  override final def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] =
    collection.get(doc._id).map {
      case Some(current) => modifying(current, doc)
      case None => adding(doc)
    }

  override final def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Unit] = {
    collection.query.filter(_ => index === value).stream.docs.foreach(removing).drain
  }

  override final def truncate(): Task[Unit] = collection.transaction { implicit transaction =>
    collection.stream.foreach(removing).drain
  }
}
