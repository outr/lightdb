package lightdb.aggregate

import fabric.Json
import lightdb.document.Document
import lightdb.index.Index
import lightdb.spatial.GeoPoint
import squants.space.Length

sealed trait AggregateFilter[D <: Document[D]] {
  def &&(that: AggregateFilter[D]): AggregateFilter[D] = (this, that) match {
    case (AggregateFilter.Combined(f1), AggregateFilter.Combined(f2)) => AggregateFilter.Combined(f1 ::: f2)
    case (_, AggregateFilter.Combined(f)) => AggregateFilter.Combined(this :: f)
    case (AggregateFilter.Combined(f), _) => AggregateFilter.Combined(f ::: List(that))
    case _ => AggregateFilter.Combined(List(this, that))
  }
}

object AggregateFilter {
  def and[D <: Document[D]](filters: AggregateFilter[D]*): AggregateFilter[D] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[F, D <: Document[D]](index: Index[F, D], value: F) extends AggregateFilter[D] {
    def getJson: Json = index.rw.read(value)
  }

  case class In[F, D <: Document[D]](index: Index[F, D], values: Seq[F]) extends AggregateFilter[D] {
    def getJson: List[Json] = values.toList.map(index.rw.read)
  }

  case class Combined[D <: Document[D]](filters: List[AggregateFilter[D]]) extends AggregateFilter[D]

  case class RangeLong[D <: Document[D]](index: Index[Long, D], from: Option[Long], to: Option[Long]) extends AggregateFilter[D]

  case class RangeDouble[D <: Document[D]](index: Index[Double, D], from: Option[Double], to: Option[Double]) extends AggregateFilter[D]

  case class Parsed[F, D <: Document[D]](index: Index[F, D], query: String, allowLeadingWildcard: Boolean) extends AggregateFilter[D]

  case class Distance[D <: Document[D]](index: Index[GeoPoint, D], from: GeoPoint, radius: Length) extends AggregateFilter[D]
}