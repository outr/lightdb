package lightdb.aggregate

import lightdb.{Query, SortDirection, Transaction}
import lightdb.doc.DocModel
import lightdb.materialized.MaterializedAggregate

case class AggregateQuery[Doc, Model <: DocModel[Doc]](query: Query[Doc, Model],
                                            functions: List[AggregateFunction[_, _, Doc]],
                                            filter: Option[AggregateFilter[Doc]] = None,
                                            sort: List[(AggregateFunction[_, _, Doc], SortDirection)] = Nil) {
  def filter(filter: AggregateFilter[Doc], and: Boolean = false): AggregateQuery[Doc, Model] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: AggregateFilter[Doc]*): AggregateQuery[Doc, Model] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(function: AggregateFunction[_, _, Doc],
           direction: SortDirection = SortDirection.Ascending): AggregateQuery[Doc, Model] = copy(
    sort = sort ::: List((function, direction))
  )

  def iterator(implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    query.collection.store.aggregate(this)

  def toList(implicit transaction: Transaction[Doc]): List[MaterializedAggregate[Doc, Model]] = iterator.toList
}
