package lightdb

import fabric.{Json, Null, bool, num, str}
import fabric.define.DefType
import fabric.rw.RW
import lightdb.aggregate.AggregateSupport
import lightdb.distance.Distance
import lightdb.filter.{Filter, FilterSupport}
import lightdb.materialized.Materializable
import lightdb.spatial.GeoPoint

sealed trait Field[Doc, V] extends FilterSupport[V, Doc, Filter[Doc]] with AggregateSupport[Doc, V] with Materializable[Doc, V] {
  implicit def rw: RW[V]

  def get: Doc => V

  override def is(value: V): Filter[Doc] = Filter.Equals(this, value)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[Doc] =
    Filter.RangeLong(this.asInstanceOf[Field[Doc, Long]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[Doc] =
    Filter.RangeDouble(this.asInstanceOf[Field[Doc, Double]], from, to)

  override def IN(values: Seq[V]): Filter[Doc] = Filter.In(this, values)

  override def parsed(query: String, allowLeadingWildcard: Boolean): Filter[Doc] =
    Filter.Parsed(this, query, allowLeadingWildcard)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): Filter[Doc] = {
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

  override def distance(from: GeoPoint, radius: Distance)
                       (implicit evidence: V =:= GeoPoint): Filter[Doc] =
    Filter.Distance(this.asInstanceOf[Field[Doc, GeoPoint]], from, radius)
}

object Field {
  case class Basic[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]
  case class Index[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]
  case class Unique[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]

  def string2Json(s: String, definition: DefType): Json = definition match {
    case DefType.Str => str(s)
    case DefType.Int => num(s.toLong)
    case DefType.Dec => num(BigDecimal(s))
    case DefType.Bool => bool(s match {
      case "1" | "true" => true
      case _ => false
    })
    case DefType.Opt(d) => if (s == null) {
      Null
    } else {
      string2Json(s, d)
    }
    case d => throw new RuntimeException(s"Unsupported DefType $d ($s)")
  }
}