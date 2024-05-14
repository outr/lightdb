package lightdb.sqlite

import fabric.Json
import lightdb.Document
import lightdb.query.Filter

case class SQLFilter[D <: Document[D]](sql: String, args: List[Json]) extends Filter[D] with SQLPart {
  override def &&(that: Filter[D]): SQLFilter[D] = {
    val tf = that.asInstanceOf[SQLFilter[D]]
    SQLFilter(s"${this.sql} AND ${tf.sql}", this.args ::: tf.args)
  }
}