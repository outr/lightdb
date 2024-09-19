package lightdb.trigger

import lightdb.Field
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction

trait BasicCollectionTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends CollectionTrigger[Doc] {
  def collection: Collection[Doc, Model]

  protected def adding(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  protected def modifying(oldDoc: Doc, newDoc: Doc)(implicit transaction: Transaction[Doc]): Unit

  protected def removing(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  override final def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = adding(doc)

  override final def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    collection.get(doc._id) match {
      case Some(current) => modifying(current, doc)
      case None => adding(doc)
    }
  }

  override final def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Unit = {
    collection.query.filter(_ => index === value).iterator.foreach(removing)
  }

  override final def truncate(): Unit = collection.transaction { implicit transaction =>
    collection.iterator.foreach(removing)
  }
}
