package lightdb.sql

import lightdb.doc.{Document, DocumentModel}

import java.sql.{PreparedStatement, ResultSet}

case class SQLResults(rs: ResultSet, sql: String, ps: PreparedStatement) {
  def release[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Unit = state.returnPreparedStatement(sql, ps)
}
