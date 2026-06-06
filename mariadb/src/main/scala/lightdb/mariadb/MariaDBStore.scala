package lightdb.mariadb

import fabric.define.{DefType, Definition}
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.sql.connect.ConnectionManager
import lightdb.sql.{SQLState, SQLStore, SqlIdent}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.Task

import java.nio.file.Path
import java.sql.Connection

/**
 * `SQLStore` dialect for MySQL/MariaDB (via the MariaDB JDBC driver, which speaks to both servers).
 *
 * Dialect specifics vs. the base SQL store:
 * - Identifiers: LightDB always double-quotes identifiers, so connections run with
 *   `sql_mode=ANSI_QUOTES` (set via [[MariaDBStoreManager.config]]'s `connectionInitSql`).
 * - Types: `VARCHAR` requires a length, and `TEXT`/`BLOB` can't be a PRIMARY KEY (or indexed without
 *   a key length) — so short strings use bounded `VARCHAR`, serialized JSON uses `LONGTEXT`.
 * - Upsert: `INSERT ... ON DUPLICATE KEY UPDATE`.
 * - Like/regex/concat: the base store's defaults are already MySQL-flavored (`LIKE`, `REGEXP`,
 *   `GROUP_CONCAT`), so the transaction needs no overrides.
 *
 * Like PostgreSQL, each LightDB instance gets its own schema (a MySQL database) for isolation.
 */
class MariaDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      val connectionManager: ConnectionManager,
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      lightDB: LightDB,
                                                                      storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = MariaDBTransaction[Doc, Model]

  // MySQL/MariaDB treats a "schema" as a database; give each LightDB instance its own.
  override def supportsSchemas: Boolean = true

  private val VarcharLength = 255
  // utf8mb4-safe index prefix: 191 * 4 = 764 bytes, leaving room for additional columns in a
  // composite index within InnoDB's 3072-byte key limit.
  private val TextKeyLength = 191

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    MariaDBTransaction(this, state, parent, writeHandlerFactory)
  }

  override protected def def2Type(name: String, d: Definition): String = d.defType match {
    case DefType.Str => s"VARCHAR($VarcharLength)"
    case DefType.Json | DefType.Obj(_) | DefType.Arr(_) | DefType.Poly(_, _) => "LONGTEXT"
    case DefType.Int => "BIGINT"
    case DefType.Bool => "TINYINT"
    case DefType.Dec => "DOUBLE"
    case DefType.Opt(inner) => def2Type(name, inner)
    case dt => throw new UnsupportedOperationException(s"$name has an unsupported type: $dt")
  }

  // The base emits `_id VARCHAR NOT NULL PRIMARY KEY`; unbounded VARCHAR is invalid in MySQL, so use
  // a bounded VARCHAR for the primary key.
  override protected def createTable(tx: TX): Unit = {
    val entries = fields.collect {
      case field if !field.rw.definition.className.contains("lightdb.spatial.GeoPoint") =>
        if field == model._id then s"${SqlIdent.quote("_id")} VARCHAR($VarcharLength) NOT NULL PRIMARY KEY"
        else s"${SqlIdent.quote(field.name)} ${def2Type(field.name, field.rw.definition)}"
    }.mkString(", ")
    executeUpdate(s"CREATE TABLE IF NOT EXISTS $fqn($entries)", tx)
  }

  override protected def createUpsertSQL(): String = {
    val cols = fields.map(f => SqlIdent.quote(f.name))
    val values = fields.map(field2Value)
    val updates = fields.filterNot(_ == model._id).map { f =>
      val q = SqlIdent.quote(f.name)
      s"$q = VALUES($q)"
    }
    val updateClause = if updates.nonEmpty then updates.mkString(", ") else {
      val q = SqlIdent.quote("_id"); s"$q = VALUES($q)"
    }
    s"INSERT INTO $fqn(${cols.mkString(", ")}) VALUES(${values.mkString(", ")}) ON DUPLICATE KEY UPDATE $updateClause"
  }

  override protected def createIndexSQL(index: Field.Indexed[Doc, _]): String = indexSQL(index.name, unique = false)

  override protected def createUniqueIndexSQL(index: Field.UniqueIndex[Doc, _]): String = indexSQL(index.name, unique = true)

  // MySQL has no covering-`INCLUDE` syntax (the indexed columns are themselves covering), and LONGTEXT
  // columns in the key need a length prefix.
  override protected def createCompositeIndexSQL(compositeIndex: lightdb.CompositeIndex[Doc]): String = {
    val cols = compositeIndex.fields.map(f => columnRef(f.name)).mkString(", ")
    s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${compositeIndex.name}_idx")} ON $fqn($cols)"
  }

  private def indexSQL(fieldName: String, unique: Boolean): String = {
    val keyword = if unique then "CREATE UNIQUE INDEX" else "CREATE INDEX"
    s"$keyword IF NOT EXISTS ${SqlIdent.quote(s"${name}_${fieldName}_idx")} ON $fqn(${columnRef(fieldName)})"
  }

  // Indexes on LONGTEXT columns require a key-length prefix in MySQL.
  private def columnRef(fieldName: String): String =
    if fields.find(_.name == fieldName).exists(f => isTextType(f.rw.definition)) then s"${SqlIdent.quote(fieldName)}($TextKeyLength)"
    else SqlIdent.quote(fieldName)

  private def isTextType(d: Definition): Boolean = d.defType match {
    case DefType.Json | DefType.Obj(_) | DefType.Arr(_) | DefType.Poly(_, _) => true
    case DefType.Opt(inner) => isTextType(inner)
    case _ => false
  }

  override protected def tables(connection: Connection): Set[String] =
    queryNames(connection, "SELECT TABLE_NAME AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", lightDB.name)

  // Resolve via information_schema directly to avoid JDBC catalog/schema ambiguity on MySQL.
  override protected def indexes(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT INDEX_NAME AS n FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")
    try {
      ps.setString(1, lightDB.name)
      ps.setString(2, name)
      collect(ps)
    } finally ps.close()
  }

  private def queryNames(connection: Connection, sql: String, arg: String): Set[String] = {
    val ps = connection.prepareStatement(sql)
    try {
      ps.setString(1, arg)
      collect(ps)
    } finally ps.close()
  }

  private def collect(ps: java.sql.PreparedStatement): Set[String] = {
    val rs = ps.executeQuery()
    try {
      var set = Set.empty[String]
      while rs.next() do set += rs.getString("n").toLowerCase
      set
    } finally rs.close()
  }
}
