package lightdb.transaction

import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.FieldAndValue
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Collection, Conversion}
import lightdb.{Query, SearchResults}
import rapid.Task

trait CollectionTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  override def store: Collection[Doc, Model]

  lazy val query: Query[Doc, Model, Doc] = Query(this, Conversion.Doc())

  def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]]

  def doUpdate[V](query: Query[Doc, Model, V],
                  updates: List[FieldAndValue[Doc, _]]): Task[Int] = query.docs.stream
    .map(store.model.rw.read)
    .map { json =>
      updates.foldLeft(json)((json, fv) => fv.update(json))
    }
    .map(store.model.rw.write)
    .evalMap(upsert)
    .count

  def doDelete[V](query: Query[Doc, Model, V]): Task[Int] = query.id.stream.evalMap(delete).count

  def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int]
}