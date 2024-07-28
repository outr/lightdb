package lightdb.store

import lightdb.{Id, Indexed, Query, SearchResults, UniqueIndex}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
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
      collection.iterator.foreach { doc =>
        indexes.foreach(_.set(doc))
      }
    }
  }

  abstract override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    super.insert(doc)
    indexes.foreach(_.set(doc))
  }

  abstract override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    super.upsert(doc)
    indexes.foreach(_.set(doc))
  }

  abstract override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    super.delete(field, value)
    // TODO: Support
  }

  override def doSearch[V](query: Query[Doc, Model],
                           conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {

    ???
  }

  abstract override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val removed = super.truncate()
    indexes.foreach(_.clear())
    removed
  }
}
