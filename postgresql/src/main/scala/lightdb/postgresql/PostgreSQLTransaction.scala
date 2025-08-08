package lightdb.postgresql

import fabric._
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.{SQLArg, SQLPart, SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

import java.sql.PreparedStatement

case class PostgreSQLTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: PostgreSQLStore[Doc, Model],
                                                                                    state: SQLState[Doc, Model],
                                                                                    parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"$name ~ ?", List(SQLArg.StringArg(expression)))

  override protected def concatPrefix: String = "STRING_AGG"

  override def populate(ps: PreparedStatement, arg: Json, index: Int): Unit = arg match {
    case Bool(b, _) => ps.setBoolean(index + 1, b)
    case _ => super.populate(ps, arg, index)
  }
}