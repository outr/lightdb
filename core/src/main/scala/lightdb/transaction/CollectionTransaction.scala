package lightdb.transaction

import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.FieldAndValue
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Collection, Conversion}
import lightdb.{Query, SearchResults}
import rapid.{Pull, Task}

trait CollectionTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  override def store: Collection[Doc, Model]

  lazy val query: Query[Doc, Model, Doc] = Query(this, Conversion.Doc())

  def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]]

  /**
   * Backend-specific streaming implementation for a prepared query.
   *
   * Default behavior matches the historical LightDB behavior: offset-based pagination across pages.
   *
   * Stores can override this for more efficient streaming (ex: keyset / cursor pagination).
   */
  def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] = {
    if (query.pageSize.nonEmpty) {
      rapid.Stream.merge {
        Task.defer {
          // Use an initial query to determine total so we can compute end bounds for offset pagination.
          val totalQuery = query.copy(limit = Some(1), countTotal = true)
          doSearch(totalQuery).flatMap { results =>
            val total = results.total.getOrElse(0)
            val end = query.limit match {
              case Some(l) => math.min(l, total)
              case None => total
            }
            val pages = query.offset to end by query.pageSize.get
            val iterator = pages.iterator.map { offset =>
              rapid.Stream.force(doSearch(query.copy(offset = offset)).map(_.streamWithScore))
            }
            Task.pure(Pull.fromIterator(iterator))
          }
        }
      }
    } else {
      rapid.Stream.force(doSearch(query).map(_.streamWithScore))
    }
  }

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