package lightdb

import fabric.{Json, Null, arr, bool, num, str}
import fabric.define.DefType
import fabric.io.JsonParser
import fabric.rw._
import lightdb.aggregate.AggregateSupport
import lightdb.distance.Distance
import lightdb.doc.Document
import lightdb.filter.{Filter, FilterSupport}
import lightdb.materialized.Materializable
import lightdb.spatial.Geo

sealed class Field[Doc <: Document[Doc], V](val name: String,
                                            val get: Doc => V,
                                            val getRW: () => RW[V],
                                            val indexed: Boolean = false) extends FilterSupport[V, Doc, Filter[Doc]] with AggregateSupport[Doc, V] with Materializable[Doc, V] {
  implicit def rw: RW[V] = getRW()

  def isArr: Boolean = rw.definition match {
    case DefType.Arr(_) => true
    case _ => false
  }

  def getJson(doc: Doc): Json = get(doc).json

  override def is(value: V): Filter[Doc] = Filter.Equals(name, value)

  override def !==(value: V): Filter[Doc] = Filter.NotEquals(name, value)

  override def regex(expression: String): Filter[Doc] = Filter.Regex(name, expression)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[Doc] =
    Filter.RangeLong(name, from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[Doc] =
    Filter.RangeDouble(name, from, to)

  override def IN(values: Seq[V]): Filter[Doc] = {
    Field.MaxIn.foreach { max =>
      if (values.size > max) throw new RuntimeException(s"Attempting to specify ${values.size} values for IN clause in $name, but maximum is ${Field.MaxIn}.")
    }
    Filter.In(name, values)
  }

  override def parsed(query: String, allowLeadingWildcard: Boolean): Filter[Doc] =
    Filter.Parsed(name, query, allowLeadingWildcard)

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

  def opt: Field[Doc, Option[V]] = new Field[Doc, Option[V]](name, doc => Option(get(doc)), () => implicitly[RW[Option[V]]], indexed)

  override def distance(from: Geo.Point, radius: Distance): Filter[Doc] =
    Filter.Distance(name, from, radius)

  override def toString: String = s"Field(name = $name)"
}

trait Indexed[Doc <: Document[Doc], V] extends Field[Doc, V]

trait UniqueIndex[Doc <: Document[Doc], V] extends Indexed[Doc, V]

trait Tokenized[Doc <: Document[Doc]] extends Indexed[Doc, String]

object Field {
  val NullString: String = "||NULL||"

  var MaxIn: Option[Int] = Some(1_000)

  def apply[Doc <: Document[Doc], V](name: String, get: Doc => V)(implicit getRW: => RW[V]): Field[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW
  )

  def indexed[Doc <: Document[Doc], V](name: String, get: Doc => V)(implicit getRW: => RW[V]): Indexed[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW,
    indexed = true
  ) with Indexed[Doc, V] {
    override def toString: String = s"Indexed(name = ${this.name})"
  }

  def tokenized[Doc <: Document[Doc]](name: String, get: Doc => String): Tokenized[Doc] = new Field[Doc, String](
    name = name,
    get = get,
    getRW = () => stringRW,
    indexed = true
  ) with Tokenized[Doc] {
    override def toString: String = s"Tokenized(name = ${this.name})"
  }

  def unique[Doc <: Document[Doc], V](name: String, get: Doc => V)(implicit getRW: => RW[V]): UniqueIndex[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW,
    indexed = true
  ) with UniqueIndex[Doc, V] {
    override def toString: String = s"Unique(name = ${this.name})"
  }

  def string2Json(name: String, s: String, definition: DefType): Json = definition match {
    case _ if s == null | s == NullString => Null
    case DefType.Str => str(s)
    case DefType.Int => num(s.toLong)
    case DefType.Dec => num(BigDecimal(s))
    case DefType.Bool => bool(s match {
      case "1" | "true" => true
      case _ => false
    })
    case DefType.Opt(d) => string2Json(name, s, d)
    case DefType.Enum(_, _) => str(s)
    case DefType.Arr(d) => arr(s.split(";;").toList.map(string2Json(name, _, d)): _*)
    case _ => try {
      JsonParser(s)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Failure to convert $name = $s to $definition", t)
    }
  }
}