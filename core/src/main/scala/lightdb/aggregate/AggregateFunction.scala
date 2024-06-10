package lightdb.aggregate

import fabric.rw.RW
import lightdb.Document
import lightdb.index.{FilterSupport, Index}

case class AggregateFunction[T, F, D <: Document[D]](name: String, index: Index[F, D], `type`: AggregateType)
                                                    (implicit val rw: RW[T]) extends FilterSupport[F, D, AggregateFilter[D]] {
  override implicit def fRW: RW[F] = index.fRW

  override def is(value: F): AggregateFilter[D] = index.aggregateFilterSupport(name).is(value)

  override def >(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).>(value)

  override def >=(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).>=(value)

  override def <(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).<(value)

  override def <=(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).<=(value)

  override def BETWEEN(tuple: (F, F))(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).BETWEEN(tuple)

  override def <=>(tuple: (F, F))(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).<=>(tuple)

  override def range(from: Option[F],
                     to: Option[F],
                     includeFrom: Boolean = true,
                     includeTo: Boolean = true)
                    (implicit num: Numeric[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).range(from, to, includeFrom, includeTo)

  override def rangeLong(from: Long, to: Long): AggregateFilter[D] = index.aggregateFilterSupport(name).rangeLong(from, to)

  override def rangeDouble(from: Double, to: Double): AggregateFilter[D] = index.aggregateFilterSupport(name).rangeDouble(from, to)

  override def IN(values: Seq[F]): AggregateFilter[D] = index.aggregateFilterSupport(name).IN(values)

  override def parsed(query: String, allowLeadingWildcard: Boolean = false): AggregateFilter[D] = index.aggregateFilterSupport(name).parsed(query, allowLeadingWildcard)

  override def words(s: String,
                     matchStartsWith: Boolean = true,
                     matchEndsWith: Boolean = false): AggregateFilter[D] = index.aggregateFilterSupport(name).words(s, matchStartsWith, matchEndsWith)
}