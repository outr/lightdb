package lightdb.mariadb

import lightdb.aggregate.AggregateType
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.query.{SQLPart, SQLQuery}
import lightdb.sql.{SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

/**
 * MySQL/MariaDB transaction. The base [[SQLStoreTransaction]] already emits MySQL-compatible
 * `LIKE`/`REGEXP` and maps booleans to 0/1; only the aggregate expressions need dialect tweaks.
 */
case class MariaDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: MariaDBStore[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override protected def aggExpr(`type`: AggregateType, column: String): String = `type` match {
    // MySQL `AVG` over an integer column returns a low-scale DECIMAL; cast to DOUBLE for full precision.
    case AggregateType.Avg => s"AVG(CAST($column AS DOUBLE))"
    // MySQL uses `GROUP_CONCAT(... SEPARATOR x)`, not the `STRING_AGG(col, x)` arg form. Separators
    // match what the base result parser splits on (`;;` for Concat, `,,` for ConcatDistinct).
    case AggregateType.Concat => s"GROUP_CONCAT($column SEPARATOR ';;')"
    case AggregateType.ConcatDistinct => s"GROUP_CONCAT(DISTINCT $column SEPARATOR ',,')"
    case other => super.aggExpr(other, column)
  }

  override protected def dialectQuery(sql: SQLQuery): SQLQuery = MariaDBTransaction.dialect(sql)
}

object MariaDBTransaction {
  // The DSL renders a function call as a `name(` fragment followed by its arguments, so match the
  // opening rather than the full `random()`.
  private val RandomFunction = "\\brandom\\(".r

  /** Rewrites portable DSL output into what MySQL/MariaDB accept: `RAND()` instead of `random()`, and
    * `IN (SELECT * FROM (SELECT … LIMIT n) AS x)` where MariaDB rejects a `LIMIT` directly inside an
    * `IN` subquery ("This version of MariaDB doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'"). */
  private[mariadb] def dialect(sql: SQLQuery): SQLQuery = SQLQuery(rewriteParts(sql.parts))

  private def rewriteParts(parts: List[SQLPart]): List[SQLPart] = parts match {
    case SQLPart.Fragment(open) :: (sub: SQLQuery) :: SQLPart.Fragment(")") :: rest
      if open.endsWith(" IN (") && hasTopLevelLimit(sub) =>
      val derived = SQLQuery(List(SQLPart.Fragment("SELECT * FROM ("), dialect(sub), SQLPart.Fragment(") AS lightdb_in")))
      SQLPart.Fragment(open) :: derived :: SQLPart.Fragment(")") :: rewriteParts(rest)
    case (sub: SQLQuery) :: rest => dialect(sub) :: rewriteParts(rest)
    case SQLPart.Fragment(value) :: rest => SQLPart.Fragment(RandomFunction.replaceAllIn(value, "RAND(")) :: rewriteParts(rest)
    case other :: rest => other :: rewriteParts(rest)
    case Nil => Nil
  }

  private def hasTopLevelLimit(sub: SQLQuery): Boolean = sub.parts.exists {
    case SQLPart.Fragment(" LIMIT ") => true
    case _ => false
  }
}
