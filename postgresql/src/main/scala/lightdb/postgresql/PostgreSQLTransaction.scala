package lightdb.postgresql

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.{SQLArg, SQLPart, SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

case class PostgreSQLTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: PostgreSQLStore[Doc, Model],
                                                                                    state: SQLState[Doc, Model],
                                                                                    parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"$name ~ ?", List(SQLArg.StringArg(expression)))

  override protected def concatPrefix: String = "STRING_AGG"
}