package lightdb.filter

import fabric.Json
import lightdb.Field
import lightdb.spatial.GeoPoint

sealed trait Filter[Doc] {
  def fields: List[Field[Doc, _]]

  def &&(that: Filter[Doc]): Filter[Doc] = (this, that) match {
    case (Filter.Combined(f1), Filter.Combined(f2)) => Filter.Combined(f1 ::: f2)
    case (_, Filter.Combined(f)) => Filter.Combined(this :: f)
    case (Filter.Combined(f), _) => Filter.Combined(f ::: List(that))
    case _ => Filter.Combined(List(this, that))
  }
}

object Filter {
  def and[Doc](filters: Filter[Doc]*): Filter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc, F](field: Field[Doc, F], value: F) extends Filter[Doc] {
    def getJson: Json = field.rw.read(value)

    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class In[Doc, F](field: Field[Doc, F], values: Seq[F]) extends Filter[Doc] {
    def getJson: List[Json] = values.toList.map(field.rw.read)

    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class Combined[Doc](filters: List[Filter[Doc]]) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = filters.flatMap(_.fields)
  }

  case class RangeLong[Doc](field: Field[Doc, Long], from: Option[Long], to: Option[Long]) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class RangeDouble[Doc](field: Field[Doc, Double], from: Option[Double], to: Option[Double]) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class Parsed[Doc, F](field: Field[Doc, F], query: String, allowLeadingWildcard: Boolean) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class Distance[Doc](field: Field[Doc, Option[GeoPoint]], from: GeoPoint, radius: lightdb.distance.Distance) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }
}