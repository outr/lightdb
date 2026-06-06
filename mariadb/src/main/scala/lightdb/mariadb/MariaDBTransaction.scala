package lightdb.mariadb

import lightdb.aggregate.AggregateType
import lightdb.doc.{Document, DocumentModel}
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
}
