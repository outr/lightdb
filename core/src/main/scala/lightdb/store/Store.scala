package lightdb.store

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{DocModel, Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb.{Field, Id, Query, SearchResults}

abstract class Store[Doc, Model <: DocModel[Doc]] {
  protected var collection: Collection[Doc, Model] = _

  protected def id(doc: Doc): Id[Doc] = doc.asInstanceOf[Document[_]]._id.asInstanceOf[Id[Doc]]
  protected lazy val idField: Field.Unique[Doc, Id[Doc]] = collection.model.asInstanceOf[DocumentModel[_]]._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]]

  def storeMode: StoreMode

  def init(collection: Collection[Doc, Model]): Unit = {
    this.collection = collection
  }

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