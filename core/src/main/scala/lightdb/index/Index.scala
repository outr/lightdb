package lightdb.index

import fabric._
import fabric.define.DefType
import fabric.rw.{Convertible, RW}
import lightdb.aggregate.AggregateFilter
import lightdb.document.Document
import lightdb.filter.{AggregateSupport, Filter, FilterSupport}
import lightdb.spatial.GeoPoint
import squants.space.Length

case class Index[F, D <: Document[D]](name: String,
                                      get: D => List[F],
                                      store: Boolean,
                                      sorted: Boolean,
                                      tokenized: Boolean,
                                      aggregate: String => FilterSupport[F, D, AggregateFilter[D]])
                                     (implicit val rw: RW[F]) extends FilterSupport[F, D, Filter[D]] with AggregateSupport[F, D] with Materializable[D, F] {
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)

  override def is(value: F): Filter[D] = Filter.Equals(this, value)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[D] =
    Filter.RangeLong(this.asInstanceOf[Index[Long, D]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[D] =
    Filter.RangeDouble(this.asInstanceOf[Index[Double, D]], from, to)

  override def IN(values: Seq[F]): Filter[D] = Filter.In(this, values)

  override def parsed(query: String, allowLeadingWildcard: Boolean): Filter[D] =
    Filter.Parsed(this, query, allowLeadingWildcard)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): Filter[D] = {
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

  override def distance(from: GeoPoint, radius: Length)
                       (implicit evidence: F =:= GeoPoint): Filter[D] =
    Filter.Distance(this.asInstanceOf[Index[GeoPoint, D]], from, radius)
}

object Index {
  def string2Json[F](s: String)(rw: RW[F]): Json = rw.definition match {
    case DefType.Str => str(s)
    case DefType.Int => num(s.toLong)
    case DefType.Dec => num(BigDecimal(s))
    case DefType.Bool => bool(s.toBoolean)
    case d => throw new RuntimeException(s"Unsupported DefType $d ($s)")
  }
}