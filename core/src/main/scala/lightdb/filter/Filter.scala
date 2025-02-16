package lightdb.filter

import fabric.{Json, Str}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.spatial.Geo

sealed trait Filter[Doc <: Document[Doc]] {
  def fieldNames: List[String]

  def fields(model: DocumentModel[Doc]): List[Field[Doc, _]] =
    fieldNames.map(model.fieldByName)
}

object Filter {
  def and[Doc <: Document[Doc]](filters: Filter[Doc]*): Filter[Doc] = filters.tail
    .foldLeft(filters.head)((combined, filter) => combined && filter)

  case class Equals[Doc <: Document[Doc], F](fieldName: String, value: F) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = model.fieldByName[F](fieldName).rw.read(value)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object Equals {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], value: F): Equals[Doc, F] = Equals(field.name, value)
  }

  case class NotEquals[Doc <: Document[Doc], F](fieldName: String, value: F) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = model.fieldByName[F](fieldName).rw.read(value)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object NotEquals {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], value: F): NotEquals[Doc, F] = NotEquals(field.name, value)
  }

  case class Regex[Doc <: Document[Doc], F](fieldName: String, expression: String) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): Json = Str(expression)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object Regex {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], expression: String): Regex[Doc, F] = Regex(field.name, expression)
  }

  case class In[Doc <: Document[Doc], F](fieldName: String, values: Seq[F]) extends Filter[Doc] {
    def getJson(model: DocumentModel[Doc]): List[Json] = values.toList.map(model.fieldByName[F](fieldName).rw.read)
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)

    override lazy val fieldNames: List[String] = List(fieldName)
  }
  object In {
    def apply[Doc <: Document[Doc], F](field: Field[Doc, F], values: Seq[F]): In[Doc, F] = In(field.name, values)
  }

  case class RangeLong[Doc <: Document[Doc]](fieldName: String, from: Option[Long], to: Option[Long]) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Long] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class RangeDouble[Doc <: Document[Doc]](fieldName: String, from: Option[Double], to: Option[Double]) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Double] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class StartsWith[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class EndsWith[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Contains[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Exact[Doc <: Document[Doc], F](fieldName: String, query: String) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, F] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Distance[Doc <: Document[Doc]](fieldName: String, from: Geo.Point, radius: lightdb.distance.Distance) extends Filter[Doc] {
    def field(model: DocumentModel[Doc]): Field[Doc, Geo] = model.fieldByName(fieldName)
    override lazy val fieldNames: List[String] = List(fieldName)
  }

  case class Multi[Doc <: Document[Doc]](minShould: Int, filters: List[FilterClause[Doc]] = Nil) extends Filter[Doc] {
    def conditional(filter: Filter[Doc], condition: Condition, boost: Option[Double] = None): Multi[Doc] =
      copy(filters = filters ::: List(FilterClause(filter, condition, boost)))

    override def fieldNames: List[String] = filters.flatMap(_.filter.fieldNames)
  }

  case class DrillDownFacetFilter[Doc <: Document[Doc]](fieldName: String, path: List[String], showOnlyThisLevel: Boolean = false) extends Filter[Doc] {
    override lazy val fieldNames: List[String] = List(fieldName)

    /**
     * Only returns facets that represent this as the lowest level. If there's another level below this, it will be
     * excluded from the result set.
     */
    lazy val onlyThisLevel: DrillDownFacetFilter[Doc] = copy(showOnlyThisLevel = true)
  }
}