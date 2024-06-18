package lightdb.query

import lightdb.Document

trait Filter[D <: Document[D]] {
  def &&(that: Filter[D]): Filter[D]
}

object Filter {
  def and[D <: Document[D]](filters: Filter[D]*): Filter[D] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)
}