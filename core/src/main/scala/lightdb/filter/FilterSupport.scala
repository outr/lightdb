package lightdb.filter

import fabric.rw.{Convertible, RW}
import fabric.{NumDec, NumInt}
import lightdb.distance.Distance
import lightdb.spatial.Geo

trait FilterSupport[F, Doc, Filter] {
  implicit def rw: RW[F]

  protected def doublePrecision = 0.0001

  def ===(value: F): Filter = is(value)
  def is(value: F): Filter

  def !==(value: F): Filter

  def >(value: F)(implicit num: Numeric[F]): Filter = range(Some(value), None, includeFrom = false)
  def >=(value: F)(implicit num: Numeric[F]): Filter = range(Some(value), None)
  def <(value: F)(implicit num: Numeric[F]): Filter = range(None, Some(value), includeTo = false)
  def <=(value: F)(implicit num: Numeric[F]): Filter = range(None, Some(value))

  def ~*(expression: String): Filter = regex(expression)
  def regex(expression: String): Filter

  def startsWith(value: String): Filter
  def endsWith(value: String): Filter
  def contains(value: String): Filter
  def exactly(value: String): Filter

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
        from.map(l => l.toLong + (if (includeFrom) 0 else 1)),
        to.map(l => l.toLong - (if (includeTo) 0 else 1))
      )
      case NumDec(_, _) => rangeDouble(
        from.map(d => d.toDouble + (if (includeFrom) 0.0 else doublePrecision)),
        to.map(d => d.toDouble - (if (includeTo) 0.0 else doublePrecision))
      )
      case json => throw new UnsupportedOperationException(s"Unsupported value for range query: $json")
    }
  }

  protected def rangeLong(from: Option[Long], to: Option[Long]): Filter

  protected def rangeDouble(from: Option[Double], to: Option[Double]): Filter

  def in(values: Seq[F]): Filter

  def words(s: String,
            matchStartsWith: Boolean = true,
            matchEndsWith: Boolean = false): Filter

  def distance(from: Geo.Point, radius: Distance): Filter
}

object FilterSupport {
  def rangeLong[F, Doc, Filter](filter: FilterSupport[F, Doc, Filter],
                                             from: Option[Long],
                                             to: Option[Long]): Filter = filter.rangeLong(from, to)

  def rangeDouble[F, Doc, Filter](filter: FilterSupport[F, Doc, Filter],
                                             from: Option[Double],
                                             to: Option[Double]): Filter = filter.rangeDouble(from, to)
}
