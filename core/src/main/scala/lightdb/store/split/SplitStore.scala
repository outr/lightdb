package lightdb.store.split

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.{Field, Query, SearchResults}

import scala.language.implicitConversions

case class SplitStore[Doc, Model <: DocModel[Doc]](storage: Store[Doc, Model],
                                                   searching: Store[Doc, Model],
                                                   storeMode: StoreMode) extends Store[Doc, Model] {
  private implicit def transaction2Split(transaction: Transaction[Doc]): SplitTransaction[Doc] = transaction.asInstanceOf[SplitTransaction[Doc]]

  override def init(collection: Collection[Doc, Model]): Unit = {
    storage.init(collection)
    searching.init(collection)
  }

  override def createTransaction(): Transaction[Doc] = SplitTransaction(
    storage = storage.createTransaction(),
    searching = searching.createTransaction()
  )

  override def releaseTransaction(transaction: Transaction[Doc]): Unit = {
    storage.releaseTransaction(transaction.storage)
    searching.releaseTransaction(transaction.searching)
  }

  override def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    storage.set(doc)(transaction.storage)
    searching.set(doc)(transaction.searching)
  }

  override def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] = {
    storage.get(field, value)(transaction.storage)
  }

  override def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    storage.delete(field, value)(transaction.storage)
    searching.delete(field, value)(transaction.searching)
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    storage.count(transaction.storage)
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = {
    storage.iterator(transaction.storage)
  }

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {
    searching.doSearch[V](query, conversion)(transaction.searching)
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] = {
    searching.aggregate(query)(transaction.searching)
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int = {
    searching.aggregateCount(query)(transaction.searching)
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    storage.truncate()(transaction.storage)
    searching.truncate()(transaction.searching)
  }

  override def dispose(): Unit = {
    storage.dispose()
    searching.dispose()
  }
}