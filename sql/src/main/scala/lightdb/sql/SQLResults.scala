package lightdb.sql

import lightdb.doc.Document

import java.sql.{PreparedStatement, ResultSet}

case class SQLResults(rs: ResultSet, sql: String, ps: PreparedStatement) {
  def release[Doc <: Document[Doc]](state: SQLState[Doc]): Unit = state.returnPreparedStatement(sql, ps)
}
