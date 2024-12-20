package lightdb.aggregate

import lightdb.{Query, SortDirection}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction

case class AggregateQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](query: Query[Doc, Model],
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

  def count(implicit transaction: Transaction[Doc]): Int =
    query.store.aggregateCount(this)

  def iterator(implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    query.store.aggregate(this)

  def toList(implicit transaction: Transaction[Doc]): List[MaterializedAggregate[Doc, Model]] = iterator.toList
}
