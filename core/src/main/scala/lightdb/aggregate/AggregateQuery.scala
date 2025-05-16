package lightdb.aggregate

import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb.{Query, SortDirection}
import rapid.Task

case class AggregateQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](query: Query[Doc, Model, _],
                                                                             functions: List[AggregateFunction[_, _, Doc]],
                                                                             filter: Option[AggregateFilter[Doc]] = None,
                                                                             sort: List[(AggregateFunction[_, _, Doc], SortDirection)] = Nil) {
  def filter(f: Model => AggregateFilter[Doc], and: Boolean = false): AggregateQuery[Doc, Model] = {
    val filter = f(query.model)
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(f: Model => List[AggregateFilter[Doc]]): AggregateQuery[Doc, Model] = {
    val filters = f(query.model)
    if (filters.nonEmpty) {
      var filter = filters.head
      filters.tail.foreach { f =>
        filter = filter && f
      }
      this.filter(_ => filter)
    } else {
      this
    }
  }

  def sort(f: Model => AggregateFunction[_, _, Doc],
           direction: SortDirection = SortDirection.Ascending): AggregateQuery[Doc, Model] = copy(
    sort = sort ::: List((f(query.model), direction))
  )

  def count: Task[Int] =
    query.transaction.aggregateCount(this)

  def stream: rapid.Stream[MaterializedAggregate[Doc, Model]] =
    query.transaction.aggregate(this)

  def toList: Task[List[MaterializedAggregate[Doc, Model]]] = stream.toList
}
