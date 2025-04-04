package lightdb.store

import lightdb.{LightDB, Query, SearchResults}
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import rapid.Task

import java.nio.file.Path

abstract class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                             path: Option[Path],
                                                                             model: Model,
                                                                             lightDB: LightDB,
                                                                             storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  lazy val query: Query[Doc, Model, Doc] = Query(model, this, Conversion.Doc())

  def doSearch[V](query: Query[Doc, Model, V])
                 (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]]

  def aggregate(query: AggregateQuery[Doc, Model])
               (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int]
}
