package lightdb.aggregate

import lightdb.document.Document

trait AggregateFilter[D <: Document[D]] {
  def &&(that: AggregateFilter[D]): AggregateFilter[D]
}

object AggregateFilter {
  def and[D <: Document[D]](filters: AggregateFilter[D]*): AggregateFilter[D] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)
}