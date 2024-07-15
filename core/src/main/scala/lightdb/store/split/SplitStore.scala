package lightdb.store.split

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import lightdb.{Field, Query, SearchResults}

import scala.language.implicitConversions

case class SplitStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](storage: Store[Doc, Model],
                                                                         searching: Store[Doc, Model],
                                                                         storeMode: StoreMode) extends Store[Doc, Model] {
  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
    storage.init(collection)
    searching.init(collection)
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = {
    storage.prepareTransaction(transaction)
    searching.prepareTransaction(transaction)
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    storage.insert(doc)
    searching.insert(doc)
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    storage.upsert(doc)
    searching.upsert(doc)
  }

  override def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] = {
    storage.get(field, value)
  }

  override def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    storage.delete(field, value)
    searching.delete(field, value)
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    storage.count
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = {
    storage.iterator
  }

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {
    searching.doSearch[V](query, conversion)
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] = {
    searching.aggregate(query)
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int = {
    searching.aggregateCount(query)
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    storage.truncate()
    searching.truncate()
  }

  override def dispose(): Unit = {
    storage.dispose()
    searching.dispose()
  }
}