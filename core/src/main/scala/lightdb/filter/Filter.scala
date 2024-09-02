package lightdb.filter

import fabric.Json
import lightdb.Field
import lightdb.doc.{Document, DocumentModel}
import lightdb.spatial.GeoPoint

sealed trait Filter[Doc <: Document[Doc]] {
  def fields: List[Field[Doc, _]]
}

object Filter {
  def and[Doc <: Document[Doc]](filters: Filter[Doc]*): Filter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc <: Document[Doc], F](field: Field[Doc, F], value: F) extends Filter[Doc] {
    def getJson: Json = field.rw.read(value)

    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class NotEquals[Doc <: Document[Doc], F](field: Field[Doc, F], value: F) extends Filter[Doc] {
    def getJson: Json = field.rw.read(value)

    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class In[Doc <: Document[Doc], F](field: Field[Doc, F], values: Seq[F]) extends Filter[Doc] {
    def getJson: List[Json] = values.toList.map(field.rw.read)

    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class RangeLong[Doc <: Document[Doc]](field: Field[Doc, Long], from: Option[Long], to: Option[Long]) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class RangeDouble[Doc <: Document[Doc]](field: Field[Doc, Double], from: Option[Double], to: Option[Double]) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class Parsed[Doc <: Document[Doc], F](field: Field[Doc, F], query: String, allowLeadingWildcard: Boolean) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }

  case class Distance[Doc <: Document[Doc]](field: Field[Doc, Option[GeoPoint]], from: GeoPoint, radius: lightdb.distance.Distance) extends Filter[Doc] {
    override lazy val fields: List[Field[Doc, _]] = List(field)
  }
  case class Multi[Doc <: Document[Doc]](minShould: Int, filters: List[FilterClause[Doc]] = Nil) extends Filter[Doc] {
    def conditional(filter: Filter[Doc], condition: Condition, boost: Option[Double] = None): Multi[Doc] =
      copy(filters = filters ::: List(FilterClause(filter, condition, boost)))

    override def fields: List[Field[Doc, _]] = filters.flatMap(_.filter.fields)
  }
}