package lightdb.filter

import fabric.Json
import lightdb.document.Document
import lightdb.index.Index

trait Filter[D <: Document[D]] {
  def &&(that: Filter[D]): Filter[D] = (this, that) match {
    case (CombinedFilter(f1), CombinedFilter(f2)) => CombinedFilter(f1 ::: f2)
    case (_, CombinedFilter(f)) => CombinedFilter(this :: f)
    case (CombinedFilter(f), _) => CombinedFilter(f ::: List(that))
    case _ => CombinedFilter(List(this, that))
  }
}

object Filter {
  def and[D <: Document[D]](filters: Filter[D]*): Filter[D] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)
}

case class EqualsFilter[F, D <: Document[D]](index: Index[F, D], value: F) extends Filter[D] {
  def getJson: Json = index.rw.read(value)
}

case class InFilter[F, D <: Document[D]](index: Index[F, D], values: Seq[F]) extends Filter[D] {
  def getJson: List[Json] = values.toList.map(index.rw.read)
}

case class CombinedFilter[D <: Document[D]](filters: List[Filter[D]]) extends Filter[D]

case class RangeLongFilter[D <: Document[D]](index: Index[Long, D], from: Long, to: Long) extends Filter[D]

case class RangeDoubleFilter[D <: Document[D]](index: Index[Double, D], from: Double, to: Double) extends Filter[D]