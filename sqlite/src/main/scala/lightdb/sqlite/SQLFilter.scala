package lightdb.sqlite

import fabric.Json
import lightdb.Document
import lightdb.query.Filter

case class SQLFilter[D <: Document[D]](sql: String, args: List[Json]) extends Filter[D] with SQLPart {
  def &&(that: SQLFilter[D]): SQLFilter[D] = SQLFilter(s"${this.sql} AND ${that.sql}", this.args ::: that.args)
}