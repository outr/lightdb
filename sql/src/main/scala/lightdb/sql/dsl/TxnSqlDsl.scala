package lightdb.sql.dsl

import fabric.rw.RW
import lightdb.ListExtras
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.sql.SQLStoreTransaction
import lightdb.sql.query.{SQLPart, SQLQuery}
import rapid.Task

/**
 * Transaction-aware SQL DSL.
 *
 * - Table is determined by the bound [[SQLStoreTransaction]] (uses `txn.fqn`)
 * - Columns/filters are type-safe via the transaction's `Model`
 *
 * Intended usage:
 * {{{
 * txn.sql
 *   .columns(m => m.name)
 *   .where(m => m.name === "Adam")
 *   .as[Name]
 *   .stream
 * }}}
 */
object TxnSqlDsl {
  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](txn: SQLStoreTransaction[Doc, Model]): Builder[Doc, Model] =
    Builder(txn = txn)

  final case class Results[V](stream: rapid.Stream[V]) {
    def list: Task[List[V]] = stream.toList
  }

  final case class Builder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    txn: SQLStoreTransaction[Doc, Model],
    selected: List[Field[Doc, _]] = Nil,
    filters: List[Filter[Doc]] = Nil
  ) {
    def columns(fields: (Model => Field[Doc, _])*): Builder[Doc, Model] =
      copy(selected = selected ::: fields.toList.map(_(txn.store.model)))

    def where(f: Model => Filter[Doc]): Builder[Doc, Model] =
      copy(filters = filters ::: List(f(txn.store.model)))

    def andWhere(f: Model => Filter[Doc]): Builder[Doc, Model] = where(f)

    /**
     * Builds the SQLQuery and returns results with a lazily-evaluated stream.
     */
    def as[V](implicit rw: RW[V]): Results[V] = {
      val query = toSQLQuery
      Results[V](txn.sqlStream[V](query))
    }

    def toSQLQuery: SQLQuery = {
      val fieldParts: List[SQLPart] =
        if selected.isEmpty then List(SQLPart.Fragment("*"))
        else selected.map(f => SQLPart.Fragment(f.name)).intersperse(SQLPart.Fragment(", "))

      val whereParts: List[SQLPart] =
        if filters.isEmpty then Nil
        else {
          val parts = filters.map(txn.filterToSQLPart)
          SQLPart.Fragment(" WHERE ") :: parts.intersperse(SQLPart.Fragment(" AND "))
        }

      SQLQuery(
        List(SQLPart.Fragment("SELECT ")) :::
          fieldParts :::
          List(SQLPart.Fragment(" FROM "), SQLPart.Fragment(txn.fqn)) :::
          whereParts
      )
    }
  }
}


