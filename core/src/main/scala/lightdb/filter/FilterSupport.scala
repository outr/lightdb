package lightdb.filter

import fabric.rw.{Convertible, RW}
import fabric.{NumDec, NumInt}
import lightdb.document.Document

trait FilterSupport[F, D <: Document[D], Filter] {
  implicit def rw: RW[F]

  protected def doublePrecision = 0.0001

  def ===(value: F): Filter = is(value)
  def is(value: F): Filter

  def >(value: F)(implicit num: Numeric[F]): Filter = range(Some(value), None, includeFrom = false)
  def >=(value: F)(implicit num: Numeric[F]): Filter = range(Some(value), None)
  def <(value: F)(implicit num: Numeric[F]): Filter = range(None, Some(value), includeTo = false)
  def <=(value: F)(implicit num: Numeric[F]): Filter = range(None, Some(value))

  def BETWEEN(tuple: (F, F))(implicit num: Numeric[F]): Filter = range(Some(tuple._1), Some(tuple._2))
  def <=>(tuple: (F, F))(implicit num: Numeric[F]): Filter = range(Some(tuple._1), Some(tuple._2))

  def range(from: Option[F],
            to: Option[F],
            includeFrom: Boolean = true,
            includeTo: Boolean = true)
           (implicit num: Numeric[F]): Filter = {
    import num._

    from
      .orElse(to)
      .getOrElse(throw new NullPointerException("Range must have at least one specified (from and/or to)"))
      .json match {
      case NumInt(_, _) => rangeLong(
        from.map(l => l.toLong + (if (includeFrom) 0 else 1)).getOrElse(Long.MinValue),
        to.map(l => l.toLong - (if (includeTo) 0 else 1)).getOrElse(Long.MaxValue)
      )
      case NumDec(_, _) => rangeDouble(
        from.map(d => d.toDouble + (if (includeFrom) 0.0 else doublePrecision)).getOrElse(Double.MinValue),
        to.map(d => d.toDouble - (if (includeTo) 0.0 else doublePrecision)).getOrElse(Double.MaxValue)
      )
      case json => throw new UnsupportedOperationException(s"Unsupported value for range query: $json")
    }
  }

  def rangeLong(from: Long, to: Long): Filter

  def rangeDouble(from: Double, to: Double): Filter

  def IN(values: Seq[F]): Filter

  def parsed(query: String, allowLeadingWildcard: Boolean = false): Filter = throw new UnsupportedOperationException("Parsed is not supported on this index")

  def words(s: String,
            matchStartsWith: Boolean = true,
            matchEndsWith: Boolean = false): Filter = throw new UnsupportedOperationException("Words is not supported on this index")
}
