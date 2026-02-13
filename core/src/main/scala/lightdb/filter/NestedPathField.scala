package lightdb.filter

import fabric.rw.RW
import lightdb.doc.Document
import lightdb.distance.Distance
import lightdb.spatial.Point

final class NestedPathField[Doc <: Document[Doc], V](val fieldName: String)(implicit val rw: RW[V])
  extends FilterSupport[V, Doc, Filter[Doc]] {
  override def is(value: V): Filter[Doc] = value match {
    case s: String => Filter.Exact(fieldName, s)
    case d: Double => Filter.RangeDouble(fieldName, Some(d), Some(d))
    case f: Float => Filter.RangeDouble(fieldName, Some(f.toDouble), Some(f.toDouble))
    case l: Long => Filter.RangeLong(fieldName, Some(l), Some(l))
    case i: Int => Filter.RangeLong(fieldName, Some(i.toLong), Some(i.toLong))
    case sh: Short => Filter.RangeLong(fieldName, Some(sh.toLong), Some(sh.toLong))
    case b: Byte => Filter.RangeLong(fieldName, Some(b.toLong), Some(b.toLong))
    case _ => Filter.Equals(fieldName, value)
  }

  override def !==(value: V): Filter[Doc] = Filter.NotEquals(fieldName, value)

  override def regex(expression: String): Filter[Doc] = Filter.Regex(fieldName, expression)

  override def startsWith(value: String): Filter[Doc] = Filter.StartsWith(fieldName, value)
  override def endsWith(value: String): Filter[Doc] = Filter.EndsWith(fieldName, value)
  override def contains(value: String): Filter[Doc] = Filter.Contains(fieldName, value)
  override def exactly(value: String): Filter[Doc] = Filter.Exact(fieldName, value)

  override def group(minShould: Int, filters: (Filter[Doc], Condition)*): Filter[Doc] =
    Filter.Multi(
      minShould = minShould,
      filters = filters.map {
        case (filter, condition) => FilterClause(filter, condition, None)
      }.toList
    )

  override protected def rangeLong(from: Option[Long], to: Option[Long]): Filter[Doc] =
    Filter.RangeLong(fieldName, from, to)

  override protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter[Doc] =
    Filter.RangeDouble(fieldName, from, to)

  override def in(values: Seq[V]): Filter[Doc] = Filter.In(fieldName, values)

  override def words(s: String, matchStartsWith: Boolean, matchEndsWith: Boolean): Filter[Doc] = {
    val words = s.split("\\s+").map { w =>
      if matchStartsWith && matchEndsWith then {
        contains(w)
      } else if matchStartsWith then {
        startsWith(w)
      } else if matchEndsWith then {
        endsWith(w)
      } else {
        exactly(w)
      }
    }.toList
    val filters = words.map(filter => FilterClause(filter, Condition.Must, None))
    Filter.Multi(minShould = 0, filters = filters)
  }

  override def distance(from: Point, radius: Distance): Filter[Doc] =
    Filter.Distance(fieldName, from, radius)

  override def range(from: Option[V],
                     to: Option[V],
                     includeFrom: Boolean = true,
                     includeTo: Boolean = true)
                    (implicit num: Numeric[V]): Filter[Doc] = {
    val probe = from.orElse(to).getOrElse(throw new NullPointerException("Range must have at least one specified (from and/or to)"))
    probe match {
      case _: Byte | _: Short | _: Int | _: Long =>
        rangeLong(
          from.map(v => num.toLong(v) + (if includeFrom then 0 else 1)),
          to.map(v => num.toLong(v) - (if includeTo then 0 else 1))
        )
      case _ =>
        rangeDouble(
          from.map(v => num.toDouble(v) + (if includeFrom then 0.0 else doublePrecision)),
          to.map(v => num.toDouble(v) - (if includeTo then 0.0 else doublePrecision))
        )
    }
  }
}

