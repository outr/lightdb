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

sealed class Field[Doc, V](val name: String,
                           val get: Doc => V,
                           val getRW: () => RW[V]) extends FilterSupport[V, Doc, Filter[Doc]] with AggregateSupport[Doc, V] with Materializable[Doc, V] {
  implicit def rw: RW[V] = getRW()

  def getJson(doc: Doc): Json = get(doc).json

  lazy val indexed: Boolean = this.isInstanceOf[Indexed[_, _]]

  override def is(value: V): Filter[Doc] = Filter.Equals(this, value)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[Doc] =
    Filter.RangeLong(this.asInstanceOf[Field[Doc, Long]], from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[Doc] =
    Filter.RangeDouble(this.asInstanceOf[Field[Doc, Double]], from, to)

  override def IN(values: Seq[V]): Filter[Doc] = {
    Field.MaxIn.foreach { max =>
      if (values.size > max) throw new RuntimeException(s"Attempting to specify ${values.size} values for IN clause in $name, but maximum is ${Field.MaxIn}.")
    }
    Filter.In(this, values)
  }

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

trait Indexed[Doc, V] extends Field[Doc, V]

trait UniqueIndex[Doc, V] extends Indexed[Doc, V]

trait Tokenized[Doc] extends Indexed[Doc, List[String]]

object Field {
  var MaxIn: Option[Int] = Some(1_000)

  def apply[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]): Field[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW
  )

  def indexed[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]): Indexed[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW
  ) with Indexed[Doc, V]

  def tokenized[Doc](name: String, get: Doc => List[String]): Tokenized[Doc] = new Field[Doc, List[String]](
    name = name,
    get = get,
    getRW = () => implicitly[RW[List[String]]]
  ) with Tokenized[Doc]

  def unique[Doc, V](name: String, get: Doc => V)(implicit getRW: => RW[V]): UniqueIndex[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW
  ) with UniqueIndex[Doc, V]

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