package lightdb

import fabric.{Json, Null, bool, num, str}
import fabric.define.DefType
import fabric.io.JsonParser
import fabric.rw._
import lightdb.aggregate.AggregateSupport
import lightdb.distance.Distance
import lightdb.filter.{Filter, FilterSupport}
import lightdb.materialized.Materializable
import lightdb.spatial.GeoPoint

sealed abstract class Field[Doc, V](getRW: RW[V]) extends FilterSupport[V, Doc, Filter[Doc]] with AggregateSupport[Doc, V] with Materializable[Doc, V] {
  implicit def rw: RW[V] = getRW

  def get: Doc => V

  def getJson(doc: Doc): Json = get(doc).json

  def indexed: Boolean = false

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
  case class Basic[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]) extends Field[Doc, V](getRW)
  case class Index[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]) extends Field[Doc, V](getRW) {
    override def indexed: Boolean = true
  }
  case class Unique[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]) extends Field[Doc, V](getRW) {
    override def indexed: Boolean = true
  }

  def string2Json(name: String, s: String, definition: DefType): Json = definition match {
    case _ if s == null => Null
    case DefType.Str => str(s)
    case DefType.Int => num(s.toLong)
    case DefType.Dec => num(BigDecimal(s))
    case DefType.Bool => bool(s match {
      case "1" | "true" => true
      case _ => false
    })
    case DefType.Opt(d) => string2Json(name, s, d)
    case DefType.Enum(_) => str(s)
    case _ => try {
      JsonParser(s)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Failure to convert $name = $s to $definition", t)
    }
  }
}