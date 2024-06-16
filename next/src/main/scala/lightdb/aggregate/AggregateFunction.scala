package lightdb.aggregate

import fabric.rw.RW
import lightdb.document.Document
import lightdb.filter.FilterSupport
import lightdb.index.Index

case class AggregateFunction[T, F, D <: Document[D]](name: String, index: Index[F, D], `type`: AggregateType)
                                                    (implicit val tRW: RW[T]) extends FilterSupport[F, D, AggregateFilter[D]] {
  def rename(name: String): AggregateFunction[T, F, D] = copy(name = name)

  override implicit def rw: RW[F] = index.rw

  override def is(value: F): AggregateFilter[D] = index.aggregate(name).is(value)

  override def >(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).>(value)

  override def >=(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).>=(value)

  override def <(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).<(value)

  override def <=(value: F)(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).<=(value)

  override def BETWEEN(tuple: (F, F))(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).BETWEEN(tuple)

  override def <=>(tuple: (F, F))(implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).<=>(tuple)

  override def range(from: Option[F],
                     to: Option[F],
                     includeFrom: Boolean = true,
                     includeTo: Boolean = true)
                    (implicit num: Numeric[F]): AggregateFilter[D] = index.aggregate(name).range(from, to, includeFrom, includeTo)

  override def rangeLong(from: Long, to: Long): AggregateFilter[D] = index.aggregate(name).rangeLong(from, to)

  override def rangeDouble(from: Double, to: Double): AggregateFilter[D] = index.aggregate(name).rangeDouble(from, to)

  override def IN(values: Seq[F]): AggregateFilter[D] = index.aggregate(name).IN(values)

  override def parsed(query: String, allowLeadingWildcard: Boolean = false): AggregateFilter[D] = index.aggregate(name).parsed(query, allowLeadingWildcard)

  override def words(s: String,
                     matchStartsWith: Boolean = true,
                     matchEndsWith: Boolean = false): AggregateFilter[D] = index.aggregate(name).words(s, matchStartsWith, matchEndsWith)
}