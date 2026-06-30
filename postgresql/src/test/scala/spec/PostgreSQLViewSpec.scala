package spec

import lightdb.postgresql.PostgreSQLStoreManager

/**
 * Runs the view suite on PostgreSQL, where every store is co-located in one database under this
 * spec's own schema (`fqn` is qualified by `db.name` = the spec name), so the views execute via the
 * SQL-native pushdown ([[lightdb.sql.RelationSql]]) — a cross-store JOIN the generic engine can't do
 * on per-store databases. Identical assertions to the H2 (generic-engine) run prove parity.
 */
@EmbeddedTest
class PostgreSQLViewSpec extends AbstractViewSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
}
