package lightdb.filter

import fabric.Json
import lightdb.document.Document
import lightdb.index.Index
import lightdb.spatial.GeoPoint
import squants.space.Length

sealed trait Filter[D <: Document[D]] {
  def &&(that: Filter[D]): Filter[D] = (this, that) match {
    case (Filter.Combined(f1), Filter.Combined(f2)) => Filter.Combined(f1 ::: f2)
    case (_, Filter.Combined(f)) => Filter.Combined(this :: f)
    case (Filter.Combined(f), _) => Filter.Combined(f ::: List(that))
    case _ => Filter.Combined(List(this, that))
  }
}

object Filter {
  def and[D <: Document[D]](filters: Filter[D]*): Filter[D] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[F, D <: Document[D]](index: Index[F, D], value: F) extends Filter[D] {
    def getJson: Json = index.rw.read(value)
  }

  case class In[F, D <: Document[D]](index: Index[F, D], values: Seq[F]) extends Filter[D] {
    def getJson: List[Json] = values.toList.map(index.rw.read)
  }

  case class Combined[D <: Document[D]](filters: List[Filter[D]]) extends Filter[D]

  case class RangeLong[D <: Document[D]](index: Index[Long, D], from: Option[Long], to: Option[Long]) extends Filter[D]

  case class RangeDouble[D <: Document[D]](index: Index[Double, D], from: Option[Double], to: Option[Double]) extends Filter[D]

  case class Parsed[F, D <: Document[D]](index: Index[F, D], query: String, allowLeadingWildcard: Boolean) extends Filter[D]

  case class Distance[D <: Document[D]](index: Index[GeoPoint, D], from: GeoPoint, radius: Length) extends Filter[D]
}