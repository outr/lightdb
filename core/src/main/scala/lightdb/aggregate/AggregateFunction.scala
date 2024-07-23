package lightdb.aggregate

import fabric.rw._
import lightdb.distance.Distance
import lightdb.Field
import lightdb.filter.FilterSupport
import lightdb.materialized.Materializable
import lightdb.spatial.GeoPoint

case class AggregateFunction[T, V, Doc](name: String, field: Field[Doc, V], `type`: AggregateType)
                                       (implicit val tRW: RW[T]) extends FilterSupport[V, Doc, AggregateFilter[Doc]] with Materializable[Doc, V] {
  def rename(name: String): AggregateFunction[T, V, Doc] = copy(name = name)

  override implicit def rw: RW[V] = field.rw

  override def is(value: V): AggregateFilter[Doc] = AggregateFilter.Equals(name, field, value)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): AggregateFilter[Doc] =
    AggregateFilter.RangeLong(name, field.asInstanceOf[Field[Doc, Long]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): AggregateFilter[Doc] =
    AggregateFilter.RangeDouble(name, field.asInstanceOf[Field[Doc, Double]], from, to)

  override def IN(values: Seq[V]): AggregateFilter[Doc] = AggregateFilter.In(name, field, values)

  override def parsed(query: String, allowLeadingWildcard: Boolean): AggregateFilter[Doc] =
    AggregateFilter.Parsed(name, field, query, allowLeadingWildcard)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): AggregateFilter[Doc] = {
    val words = s.split("\\s+").map { w =>
      if (matchStartsWith && matchEndsWith) {
        s"%$w%"
      } else if (matchStartsWith) {
        s"%$w"
      } else if (matchEndsWith) {
        s"$w%"
      } else {
        w
      }
    }.mkString(" ")
    parsed(words, allowLeadingWildcard = matchEndsWith)
  }

  override def distance(from: GeoPoint, radius: Distance): AggregateFilter[Doc] =
    AggregateFilter.Distance(name, this.asInstanceOf[Field[Doc, GeoPoint]], from, radius)
}