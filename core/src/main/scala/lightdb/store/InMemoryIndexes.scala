package lightdb.store

import lightdb._
import lightdb.field.Field._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.IndexingState
import lightdb.transaction.Transaction
import lightdb.util.InMemoryIndex

trait InMemoryIndexes[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  private lazy val indexMap: Map[Indexed[Doc, _], InMemoryIndex[Doc, _]] = Map(fields.collect {
    case i: Indexed[Doc, _] => (i, new InMemoryIndex(i, None))
  }: _*)
  private lazy val indexes = indexMap.values.toList

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)

    // Populate indexes
    collection.transaction { implicit transaction =>
      val state = new IndexingState
      collection.iterator.foreach { doc =>
        indexes.foreach(_.set(doc, state))
      }
    }
  }

  abstract override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    super.insert(doc)
    val state = new IndexingState
    indexes.foreach(_.set(doc, state))
  }

  abstract override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    super.upsert(doc)
    val state = new IndexingState
    indexes.foreach(_.set(doc, state))
  }

  abstract override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    super.delete(field, value)
    // TODO: Support
  }

  override def doSearch[V](query: Query[Doc, Model],
                           conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V] = {

    ???
  }

  abstract override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val removed = super.truncate()
    indexes.foreach(_.clear())
    removed
  }
}
