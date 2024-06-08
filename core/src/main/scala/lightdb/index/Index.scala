package lightdb.index

import fabric.rw.{Convertible, RW}
import fabric.{Json, Null, NumDec, NumInt}
import lightdb.Document
import lightdb.model.{AbstractCollection, Collection}
import lightdb.query.Filter

trait Index[F, D <: Document[D]] {
  implicit def rw: RW[F]

  private val doublePrecision = 0.0001

  def fieldName: String
  def indexSupport: IndexSupport[D]
  def materialize: Boolean
  def get: D => List[F]
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)

  def ===(value: F): Filter[D] = is(value)
  def is(value: F): Filter[D]

  def >(value: F)(implicit num: Numeric[F]): Filter[D] = range(Some(value), None, includeFrom = false)
  def >=(value: F)(implicit num: Numeric[F]): Filter[D] = range(Some(value), None)
  def <(value: F)(implicit num: Numeric[F]): Filter[D] = range(None, Some(value), includeTo = false)
  def <=(value: F)(implicit num: Numeric[F]): Filter[D] = range(None, Some(value))

  def BETWEEN(tuple: (F, F))(implicit num: Numeric[F]): Filter[D] = range(Some(tuple._1), Some(tuple._2))
  def <=>(tuple: (F, F))(implicit num: Numeric[F]): Filter[D] = range(Some(tuple._1), Some(tuple._2))

  def range(from: Option[F],
            to: Option[F],
            includeFrom: Boolean = true,
            includeTo: Boolean = true)
           (implicit num: Numeric[F]): Filter[D] = {
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

  protected def rangeLong(from: Long, to: Long): Filter[D]

  protected def rangeDouble(from: Double, to: Double): Filter[D]

  def IN(values: Seq[F]): Filter[D]

  def parsed(query: String, allowLeadingWildcard: Boolean = false): Filter[D] = throw new UnsupportedOperationException("Parsed is not supported on this index")

  def words(s: String,
            matchStartsWith: Boolean = true,
            matchEndsWith: Boolean = false): Filter[D] = throw new UnsupportedOperationException("Words is not supported on this index")

    indexSupport.index.register(this)
}