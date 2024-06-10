package lightdb.sql

import fabric.Json
import fabric.rw._
import lightdb.index.{FilterSupport, Index, IndexSupport}
import lightdb.Document
import lightdb.aggregate.AggregateFilter
import lightdb.query.Filter

case class SQLIndex[F, D <: Document[D]](fieldName: String,
                                         indexSupport: IndexSupport[D],
                                         get: D => List[F])(implicit val fRW: RW[F]) extends Index[F, D] with SQLFilterSupport[F, D, Filter[D]] { index =>
  override protected def createFilter(sql: String, args: List[Json]): Filter[D] = SQLFilter[D](sql, args)

  def aggregateFilterSupport(name: String): FilterSupport[F, D, AggregateFilter[D]] = new SQLAggregateFilterSupport(name)

  class SQLAggregateFilterSupport(name: String) extends SQLFilterSupport[F, D, AggregateFilter[D]] {
    override protected def createFilter(sql: String, args: List[Json]): AggregateFilter[D] = SQLAggregateFilter[D](sql, args)
    override def fieldName: String = name
    override implicit def fRW: RW[F] = index.fRW
  }
}