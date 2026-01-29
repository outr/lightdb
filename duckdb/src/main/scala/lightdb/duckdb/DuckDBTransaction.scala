package lightdb.duckdb

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.query.SQLPart
import lightdb.sql.{SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

case class DuckDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: DuckDBStore[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"regexp_matches($name, ?)", expression.json)
}