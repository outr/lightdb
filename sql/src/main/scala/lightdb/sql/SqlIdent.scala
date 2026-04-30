package lightdb.sql

/**
 * SQL identifier quoting.
 *
 * Always emits ANSI-SQL double-quoted identifiers (e.g. `"userId"`, `"limit"`). All major
 * dialects LightDB supports — SQLite, H2, DuckDB, PostgreSQL — accept this form, and it
 * sidesteps every reserved-word collision in user model field names (`limit`, `order`,
 * `user`, `from`, `select`, `desc`, `count`, `group`, `table`, …).
 *
 * Embedded double quotes inside an identifier are doubled per the ANSI rule (`a"b` →
 * `"a""b"`). In practice LightDB field names never contain `"`, but the helper handles it
 * defensively.
 *
 * Note: always-quoting makes PostgreSQL stop folding identifiers to lowercase. As long as
 * every emission path goes through this helper, that's a non-issue (the same `field.name`
 * string is the source of truth for both DDL and DML, so they always match exactly).
 */
object SqlIdent {
  /** Quote a single identifier (column, table, index, schema). */
  def quote(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  /** Quote a qualified identifier `qualifier.name` (e.g. `schema.table` or `table.column`). */
  def qualified(qualifier: String, name: String): String = s"${quote(qualifier)}.${quote(name)}"
}
