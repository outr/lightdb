package lightdb.aggregate

import fabric.{Json, Str}
import lightdb.doc.Document
import lightdb.field.Field
import lightdb.spatial.Geo

sealed trait AggregateFilter[Doc <: Document[Doc]] {
  def &&(that: AggregateFilter[Doc]): AggregateFilter[Doc] = (this, that) match {
    case (AggregateFilter.Combined(f1), AggregateFilter.Combined(f2)) => AggregateFilter.Combined(f1 ::: f2)
    case (_, AggregateFilter.Combined(f)) => AggregateFilter.Combined(this :: f)
    case (AggregateFilter.Combined(f), _) => AggregateFilter.Combined(f ::: List(that))
    case _ => AggregateFilter.Combined(List(this, that))
  }
}

object AggregateFilter {
  def and[Doc <: Document[Doc]](filters: AggregateFilter[Doc]*): AggregateFilter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc <: Document[Doc], F](name: String, field: Field[Doc, F], value: F) extends AggregateFilter[Doc] {
    def getJson: Json = field.rw.read(value)
  }

  case class NotEquals[Doc <: Document[Doc], F](name: String, field: Field[Doc, F], value: F) extends AggregateFilter[Doc] {
    def getJson: Json = field.rw.read(value)
  }

  case class Regex[Doc <: Document[Doc], F](name: String, field: Field[Doc, F], expression: String) extends AggregateFilter[Doc] {
    def getJson: Json = Str(expression)
  }

  case class In[Doc <: Document[Doc], F](name: String, field: Field[Doc, F], values: Seq[F]) extends AggregateFilter[Doc] {
    def getJson: List[Json] = values.toList.map(field.rw.read)
  }

  case class Combined[Doc <: Document[Doc]](filters: List[AggregateFilter[Doc]]) extends AggregateFilter[Doc]

  case class RangeLong[Doc <: Document[Doc]](name: String, field: Field[Doc, Long], from: Option[Long], to: Option[Long]) extends AggregateFilter[Doc]

  case class RangeDouble[Doc <: Document[Doc]](name: String, field: Field[Doc, Double], from: Option[Double], to: Option[Double]) extends AggregateFilter[Doc]

  case class Parsed[Doc <: Document[Doc], F](name: String, field: Field[Doc, F], query: String, allowLeadingWildcard: Boolean) extends AggregateFilter[Doc]

  case class Distance[Doc <: Document[Doc]](name: String, field: Field[Doc, Geo.Point], from: Geo.Point, radius: lightdb.distance.Distance) extends AggregateFilter[Doc]
}