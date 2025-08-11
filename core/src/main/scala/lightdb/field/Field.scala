package lightdb.field

import fabric.define.DefType
import fabric.io.JsonParser
import fabric.rw._
import fabric.{Json, Null, arr, bool, num, str}
import lightdb.aggregate.AggregateSupport
import lightdb.distance.Distance
import lightdb.doc.Document
import lightdb.facet.FacetValue
import lightdb.filter.Filter.DrillDownFacetFilter
import lightdb.filter.{Condition, Filter, FilterClause, FilterSupport}
import lightdb.materialized.Materializable
import lightdb.spatial.Geo

sealed class Field[Doc <: Document[Doc], V](val name: String,
                                            val get: FieldGetter[Doc, V],
                                            val getRW: () => RW[V],
                                            val indexed: Boolean = false) extends FilterSupport[V, Doc, Filter[Doc]] with AggregateSupport[Doc, V] with Materializable[Doc, V] {
  implicit def rw: RW[V] = getRW()

  def isArr: Boolean = rw.definition match {
    case DefType.Arr(_) => true
    case _ => false
  }

  lazy val className: Option[String] = {
    def lookup(d: DefType): Option[String] = d match {
      case DefType.Opt(d) => lookup(d)
      case DefType.Arr(d) => lookup(d)
      case DefType.Poly(_, cn) => cn
      case DefType.Obj(_, cn) => cn
      case _ => None
    }
    lookup(rw.definition)
  }

  lazy val isSpatial: Boolean = className.exists(_.startsWith("lightdb.spatial.Geo"))

  def isTokenized: Boolean = false

  def getJson(doc: Doc, state: IndexingState): Json = get(doc, this, state).json

  def apply(value: V): FieldAndValue[Doc, V] = FieldAndValue(this, value)

  override def is(value: V): Filter[Doc] = Filter.Equals(name, value)

  override def !==(value: V): Filter[Doc] = Filter.NotEquals(name, value)

  override def regex(expression: String): Filter[Doc] = Filter.Regex(name, expression)

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[Doc] =
    Filter.RangeLong(name, from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[Doc] =
    Filter.RangeDouble(name, from, to)

  override def in(values: Seq[V]): Filter[Doc] = {
    Field.MaxIn.foreach { max =>
      if (values.size > max) throw new RuntimeException(s"Attempting to specify ${values.size} values for IN clause in $name, but maximum is ${Field.MaxIn}.")
    }
    Filter.In(name, values)
  }

  override def startsWith(value: String): Filter[Doc] = Filter.StartsWith(name, value)
  override def endsWith(value: String): Filter[Doc] = Filter.EndsWith(name, value)
  override def contains(value: String): Filter[Doc] = Filter.Contains(name, value)
  override def exactly(value: String): Filter[Doc] = Filter.Exact(name, value)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): Filter[Doc] = {
    val words = s.split("\\s+").map { w =>
      if (matchStartsWith && matchEndsWith) {
        contains(w)
      } else if (matchStartsWith) {
        startsWith(w)
      } else if (matchEndsWith) {
        endsWith(w)
      } else {
        exactly(w)
      }
    }.toList
    val filters = words.map(filter => FilterClause(filter, Condition.Must, None))
    Filter.Multi(minShould = 0, filters = filters)
  }

  def opt: Field[Doc, Option[V]] = new Field[Doc, Option[V]](name, FieldGetter {
    case (doc, _, state) => Option(get(doc, this, state))
  }, () => implicitly[RW[Option[V]]], indexed)

  def list: Field[Doc, List[V]] = new Field[Doc, List[V]](name, FieldGetter {
    case (doc, _, state) => List(get(doc, this, state))
  }, () => implicitly[RW[List[V]]], indexed)

  override def distance(from: Geo.Point, radius: Distance): Filter[Doc] =
    Filter.Distance(name, from, radius)

  override def toString: String = s"Field(name = $name)"
}

object Field {
  val NullString: String = "||NULL||"

  var MaxIn: Option[Int] = Some(1000)

  def apply[Doc <: Document[Doc], V](name: String, get: FieldGetter[Doc, V])(implicit getRW: => RW[V]): Field[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW
  )

  def indexed[Doc <: Document[Doc], V](name: String, get: FieldGetter[Doc, V])(implicit getRW: => RW[V]): Indexed[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW,
    indexed = true
  ) with Indexed[Doc, V] {
    override def toString: String = s"Indexed(name = ${this.name})"
  }

  def tokenized[Doc <: Document[Doc]](name: String, get: FieldGetter[Doc, String]): Tokenized[Doc] = new Field[Doc, String](
    name = name,
    get = get,
    getRW = () => stringRW,
    indexed = true
  ) with Tokenized[Doc] {
    override def toString: String = s"Tokenized(name = ${this.name})"
  }

  def unique[Doc <: Document[Doc], V](name: String, get: FieldGetter[Doc, V])(implicit getRW: => RW[V]): UniqueIndex[Doc, V] = new Field[Doc, V](
    name = name,
    get = get,
    getRW = () => getRW,
    indexed = true
  ) with UniqueIndex[Doc, V] {
    override def toString: String = s"Unique(name = ${this.name})"
  }

  def facet[Doc <: Document[Doc]](name: String,
                                  get: FieldGetter[Doc, List[FacetValue]],
                                  hierarchical: Boolean,
                                  multiValued: Boolean,
                                  requireDimCount: Boolean): FacetField[Doc] = new FacetField[Doc](
    name = name,
    get = get,
    hierarchical = hierarchical,
    multiValued = multiValued,
    requireDimCount = requireDimCount
  ) {
    override def toString: String = s"FacetField(name = ${this.name}, hierarchical = ${this.hierarchical}, multiValued = ${this.multiValued}, requireDimCount = ${this.requireDimCount})"
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
    case DefType.Arr(d) if !s.startsWith("[") => arr(s.split(";;").toList.map(string2Json(name, _, d)): _*)
    case _ => try {
      JsonParser(s)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Failure to convert $name = $s to $definition", t)
    }
  }

  trait Indexed[Doc <: Document[Doc], V] extends Field[Doc, V]

  trait UniqueIndex[Doc <: Document[Doc], V] extends Indexed[Doc, V]

  trait Tokenized[Doc <: Document[Doc]] extends Indexed[Doc, String] {
    override def isTokenized: Boolean = true
  }

  class FacetField[Doc <: Document[Doc]](name: String,
                                         get: FieldGetter[Doc, List[FacetValue]],
                                         val hierarchical: Boolean,
                                         val multiValued: Boolean,
                                         val requireDimCount: Boolean) extends Field[Doc, List[FacetValue]](name, get, getRW = () => implicitly[RW[List[FacetValue]]], indexed = true) with Indexed[Doc, List[FacetValue]] {
    def drillDown(path: String*): DrillDownFacetFilter[Doc] = DrillDownFacetFilter(name, path.toList)
  }
}
