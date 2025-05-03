package lightdb.transaction

import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Collection, Conversion}
import lightdb.{Query, SearchResults}
import rapid.Task

trait CollectionTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  override def store: Collection[Doc, Model]

  lazy val query: Query[Doc, Model, Doc] = Query(this, Conversion.Doc())

  def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]]

  def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int]
}