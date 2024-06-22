//package lightdb.sql
//
//import fabric.Json
//import lightdb.Document
//import lightdb.aggregate.AggregateFilter
//import lightdb.query.Filter
//
//case class SQLAggregateFilter[D <: Document[D]](sql: String, args: List[Json]) extends AggregateFilter[D] with SQLPart {
//  override def &&(that: AggregateFilter[D]): AggregateFilter[D] = {
//    val tf = that.asInstanceOf[SQLAggregateFilter[D]]
//    SQLAggregateFilter(s"${this.sql} AND ${tf.sql}", this.args ::: tf.args)
//  }
//}