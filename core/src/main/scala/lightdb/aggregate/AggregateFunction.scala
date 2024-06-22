package lightdb.aggregate

import fabric.rw.RW
import lightdb.document.Document
import lightdb.filter.FilterSupport
import lightdb.index.{Index, Materializable}
import lightdb.spatial.GeoPoint
import squants.space.Length

case class AggregateFunction[T, F, D <: Document[D]](name: String, index: Index[F, D], `type`: AggregateType)
                                                    (implicit val tRW: RW[T]) extends FilterSupport[F, D, AggregateFilter[D]] with Materializable[D, F] {
  def rename(name: String): AggregateFunction[T, F, D] = copy(name = name)

  override implicit def rw: RW[F] = index.rw

  override def is(value: F): AggregateFilter[D] = AggregateFilter.Equals(index, value)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): AggregateFilter[D] =
    AggregateFilter.RangeLong(this.asInstanceOf[Index[Long, D]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): AggregateFilter[D] =
    AggregateFilter.RangeDouble(this.asInstanceOf[Index[Double, D]], from, to)

  override def IN(values: Seq[F]): AggregateFilter[D] = AggregateFilter.In(index, values)

  override def parsed(query: String, allowLeadingWildcard: Boolean): AggregateFilter[D] =
    AggregateFilter.Parsed(index, query, allowLeadingWildcard)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): AggregateFilter[D] = {
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

  override def distance(from: GeoPoint, radius: Length)(implicit evidence: F =:= GeoPoint): AggregateFilter[D] =
    AggregateFilter.Distance(this.asInstanceOf[Index[GeoPoint, D]], from, radius)
}