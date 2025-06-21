package lightdb.duckdb

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.{SQLArg, SQLPart, SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

case class DuckDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: DuckDBStore[Doc, Model],
                                                                                state: SQLState[Doc, Model],
                                                                                parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"regexp_matches($name, ?)", List(SQLArg.StringArg(expression)))
}