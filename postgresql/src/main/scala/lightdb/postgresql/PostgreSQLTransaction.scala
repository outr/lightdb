package lightdb.postgresql

import fabric.*
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.query.SQLPart
import lightdb.sql.{SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

import java.sql.PreparedStatement

case class PostgreSQLTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: PostgreSQLStore[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"$name ~ ?", expression.json)

  override protected def concatPrefix: String = "STRING_AGG"

  override protected def likePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name ILIKE ?", pattern.json)

  override protected def notLikePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name NOT ILIKE ?", pattern.json)

  override def populate(ps: PreparedStatement, arg: Json, index: Int): Unit = arg match {
    case Bool(b, _) => ps.setBoolean(index + 1, b)
    case _ => super.populate(ps, arg, index)
  }
}