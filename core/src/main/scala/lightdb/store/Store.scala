package lightdb.store

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb.{Field, Query, SearchResults}

abstract class Store[Doc, Model <: DocModel[Doc]] {
  def storeMode: StoreMode

  def init(collection: Collection[Doc, Model]): Unit

  def createTransaction(): Transaction[Doc]

  def releaseTransaction(transaction: Transaction[Doc]): Unit

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc]

  def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean

  def count(implicit transaction: Transaction[Doc]): Int

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc]

  def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                 (implicit transaction: Transaction[Doc]): SearchResults[Doc, V]

  def aggregate(query: AggregateQuery[Doc, Model])
               (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int

  def truncate()(implicit transaction: Transaction[Doc]): Int

  def dispose(): Unit
}