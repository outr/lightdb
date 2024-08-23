package lightdb.aggregate

import fabric.Json
import lightdb.Field
import lightdb.spatial.GeoPoint

sealed trait AggregateFilter[Doc] {
  def &&(that: AggregateFilter[Doc]): AggregateFilter[Doc] = (this, that) match {
    case (AggregateFilter.Combined(f1), AggregateFilter.Combined(f2)) => AggregateFilter.Combined(f1 ::: f2)
    case (_, AggregateFilter.Combined(f)) => AggregateFilter.Combined(this :: f)
    case (AggregateFilter.Combined(f), _) => AggregateFilter.Combined(f ::: List(that))
    case _ => AggregateFilter.Combined(List(this, that))
  }
}

object AggregateFilter {
  def and[Doc](filters: AggregateFilter[Doc]*): AggregateFilter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc, F](name: String, field: Field[Doc, F], value: F) extends AggregateFilter[Doc] {
    def getJson: Json = field.rw.read(value)
  }

  case class NotEquals[Doc, F](name: String, field: Field[Doc, F], value: F) extends AggregateFilter[Doc] {
    def getJson: Json = field.rw.read(value)
  }

  case class In[Doc, F](name: String, field: Field[Doc, F], values: Seq[F]) extends AggregateFilter[Doc] {
    def getJson: List[Json] = values.toList.map(field.rw.read)
  }

  case class Combined[Doc](filters: List[AggregateFilter[Doc]]) extends AggregateFilter[Doc]

  case class RangeLong[Doc](name: String, field: Field[Doc, Long], from: Option[Long], to: Option[Long]) extends AggregateFilter[Doc]

  case class RangeDouble[Doc](name: String, field: Field[Doc, Double], from: Option[Double], to: Option[Double]) extends AggregateFilter[Doc]

  case class Parsed[Doc, F](name: String, field: Field[Doc, F], query: String, allowLeadingWildcard: Boolean) extends AggregateFilter[Doc]

  case class Distance[Doc](name: String, field: Field[Doc, GeoPoint], from: GeoPoint, radius: lightdb.distance.Distance) extends AggregateFilter[Doc]
}