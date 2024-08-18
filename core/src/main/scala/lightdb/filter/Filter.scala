package lightdb.filter

import fabric.Json
import lightdb.Field
import lightdb.spatial.GeoPoint

sealed trait Filter[Doc] {
  def fields: List[Field[Doc, _]]

  def &&(that: Filter[Doc]): Filter[Doc] = (this, that) match {
    case (b1: Filter.Builder[Doc], b2: Filter.Builder[Doc]) if b1.minShould == b2.minShould =>
      Filter.Builder[Doc](minShould = b1.minShould, filters = b1.filters ::: b2.filters)
    case (_, b: Filter.Builder[Doc]) => b.must(this)
    case (b: Filter.Builder[Doc], _) => b.must(that)
    case _ => Filter.Builder[Doc]().must(this).must(that)
  }

  def ||(that: Filter[Doc]): Filter[Doc] = (this, that) match {
    case (b1: Filter.Builder[Doc], b2: Filter.Builder[Doc]) if b1.minShould == b2.minShould =>
      Filter.Builder[Doc](minShould = b1.minShould, filters = b1.filters ::: b2.filters)
    case (_, b: Filter.Builder[Doc]) => b.should(this)
    case (b: Filter.Builder[Doc], _) => b.should(that)
    case _ => Filter.Builder[Doc]().should(this).should(that)
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

  case class Builder[Doc](minShould: Int = 0, filters: List[FilterClause[Doc]] = Nil) extends Filter[Doc] {
    def minShould(i: Int): Builder[Doc] = copy(minShould = i)

    def withFilter(filter: Filter[Doc], condition: Condition, boost: Option[Double] = None): Builder[Doc] = copy(
      filters = filters ::: List(FilterClause(filter, condition, boost))
    )

    def must(filter: Filter[Doc], boost: Option[Double] = None): Builder[Doc] = withFilter(filter, Condition.Must, boost)
    def mustNot(filter: Filter[Doc], boost: Option[Double] = None): Builder[Doc] = withFilter(filter, Condition.MustNot, boost)
    def filter(filter: Filter[Doc], boost: Option[Double] = None): Builder[Doc] = withFilter(filter, Condition.Filter, boost)
    def should(filter: Filter[Doc], boost: Option[Double] = None): Builder[Doc] = withFilter(filter, Condition.Should, boost)

    override def fields: List[Field[Doc, _]] = filters.flatMap(_.filter.fields)
  }
}