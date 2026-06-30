package lightdb.sql

import fabric.Json
import fabric.rw.*
import lightdb.sql.query.{SQLPart, SQLQuery}
import lightdb.view.{NativeViewExecutor, Relation}
import rapid.Task

/**
 * Runs a [[Relation]] natively on a SQL database by lowering it ([[RelationSql]]) to one statement
 * and streaming the rows back as JSON (keyed by the view-model field names). Declines (returns
 * `None`) for relations [[RelationSql]] can't lower, so the engine falls back to the generic path.
 *
 * `coLocationKey` is the store's connection manager: relations whose dependency stores all share one
 * connection manager (e.g. via a shared `SQLDatabase`) are co-located and run here.
 */
class SqlViewExecutor(store: SQLStore[?, ?], val coLocationKey: AnyRef) extends NativeViewExecutor {
  override def execute(relation: Relation): Option[Task[List[Json]]] =
    RelationSql.lower(relation).map { sql =>
      // The lowered SQL inlines all literals (no bind parameters), so wrap it as a single fragment
      // rather than SQLQuery.parse — parse would mis-tokenize a `:name` inside a string literal as a
      // named placeholder.
      store.transaction(_.sqlStream[Json](SQLQuery(List(SQLPart.Fragment(sql)))).toList)
    }
}
