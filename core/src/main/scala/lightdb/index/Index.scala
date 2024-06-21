package lightdb.index

import fabric._
import fabric.define.DefType
import fabric.rw.{Convertible, RW}
import lightdb.aggregate.AggregateFilter
import lightdb.document.Document
import lightdb.filter.{EqualsFilter, Filter, FilterSupport, InFilter, RangeDoubleFilter, RangeLongFilter}

case class Index[F, D <: Document[D]](name: String,
                                      get: D => List[F],
                                      store: Boolean,
                                      sorted: Boolean,
                                      tokenized: Boolean,
                                      aggregate: String => FilterSupport[F, D, AggregateFilter[D]])
                                     (implicit val rw: RW[F]) extends FilterSupport[F, D, Filter[D]] {
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)

  override def is(value: F): Filter[D] = EqualsFilter(this, value)

  override def rangeLong(from: Long, to: Long): Filter[D] = RangeLongFilter(this.asInstanceOf[Index[Long, D]], from, to)

  override def rangeDouble(from: Double, to: Double): Filter[D] = RangeDoubleFilter(this.asInstanceOf[Index[Double, D]], from, to)

  override def IN(values: Seq[F]): Filter[D] = InFilter(this, values)
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