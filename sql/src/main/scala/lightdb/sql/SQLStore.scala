package lightdb.sql

import fabric.define.{DefType, Definition}
import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.*
import lightdb.sql.connect.ConnectionManager
import lightdb.store.*
import lightdb.store.nested.NestedQueryStore
import lightdb.store.prefix.PrefixScanningStore
import lightdb.transaction.batch.BatchConfig
import lightdb.store.write.WriteOp
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.handler.QueuedWriteHandler
import rapid.Task

import java.nio.file.Path
import java.sql.{Connection, DatabaseMetaData}
import scala.language.implicitConversions

abstract class SQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager)
    with PrefixScanningStore[Doc, Model]
    with NestedQueryStore[Doc, Model] {
  override type TX <: SQLStoreTransaction[Doc, Model]

  override def defaultBatchConfig: BatchConfig = BatchConfig.StoreNative

  override protected def defaultSharedTransactions: Int = 4

  override protected def flushOps(transaction: Transaction[Doc, Model], ops: Seq[WriteOp[Doc]]): Task[Unit] =
    transaction.asInstanceOf[TX].flushOps(ops)

  override protected def createNativeWriteHandler(transaction: Transaction[Doc, Model]): WriteHandler[Doc, Model] = {
    val maxBatch = math.max(1, Store.MaxInsertBatch)
    new QueuedWriteHandler(maxBatch, ops => flushOps(transaction, ops), chunkSize = maxBatch)
  }

  override def supportsNativeExistsChild: Boolean = false

  def supportsSchemas: Boolean = false

  /**
   * Quoted, fully-qualified table reference, e.g. `"my_schema"."Users"` or `"Users"`. Always
   * quoted so user-provided table names that collide with SQL reserved words (`order`, `user`,
   * etc.) parse correctly. See [[SqlIdent]] for the rationale.
   */
  lazy val fqn: String = if supportsSchemas then {
    SqlIdent.qualified(lightDB.name, name)
  } else {
    SqlIdent.quote(name)
  }

  protected def connectionManager: ConnectionManager

  override protected def initialize(): Task[Unit] = super.initialize().next(Task.next {
    transaction(initTransaction)
  })

  override protected def maximumConcurrency: Int = 1

  protected def createSchema(tx: TX): Unit = {
    executeUpdate(s"CREATE SCHEMA IF NOT EXISTS ${SqlIdent.quote(lightDB.name)}", tx)
  }

  protected def createTable(tx: TX): Unit = {
    val entries = fields.collect {
      case field if !field.rw.definition.className.contains("lightdb.spatial.GeoPoint") =>
        if field == model._id then {
          s"${SqlIdent.quote("_id")} VARCHAR NOT NULL PRIMARY KEY"
        } else {
          val t = def2Type(field.name, field.rw.definition)
          s"${SqlIdent.quote(field.name)} $t"
        }
    }.mkString(", ")
    executeUpdate(s"CREATE TABLE IF NOT EXISTS $fqn($entries)", tx)
  }

  protected def def2Type(name: String, d: Definition): String = d.defType match {
    case DefType.Str | DefType.Json | DefType.Obj(_) | DefType.Arr(_) | DefType.Poly(_, _) =>
      "VARCHAR"
    case DefType.Int => "BIGINT"
    case DefType.Bool => "TINYINT"
    case DefType.Dec => "DOUBLE"
    case DefType.Opt(inner) => def2Type(name, inner)
    case dt => throw new UnsupportedOperationException(s"$name has an unsupported type: $dt")
  }

  protected def addColumn(field: Field[Doc, _], tx: TX): Unit = {
    scribe.info(s"Adding column $fqn.${field.name}")
    executeUpdate(s"ALTER TABLE $fqn ADD COLUMN ${SqlIdent.quote(field.name)} ${def2Type(field.name, field.rw.definition)}", tx)
  }

  protected def initConnection(connection: Connection): Unit = {}

  protected def initTransaction(tx: TX): Task[Unit] = Task {
    connectionManager.active()
    val connection = tx.state.connectionManager.getConnection(tx.state)
    initConnection(connection)
    if supportsSchemas then {
      createSchema(tx)
    }
    val existingTables = tables(connection)
    if !existingTables.contains(name.toLowerCase) then {
      createTable(tx)
    }

    val fieldNames = fields.map(_.name.toLowerCase).toSet

    // Remove LightDB-generated indexes that no longer correspond to a model field. The naming
    // convention is `${storeName}_${columnName}_idx` (or just `${columnName}_idx` for some
    // older entries). Anything that doesn't match either pattern is left alone — those are
    // backend-managed indexes (H2's `primary_key_*`, SQLite's `sqlite_autoindex_*`, etc.) and
    // dropping them would break the schema.
    val CN1 = """.+_(.+)_idx""".r
    val CN2 = """(.+)_idx""".r
    val existingIndexes = indexes(connection)
    existingIndexes.filterNot(_.contains("autoindex")).foreach { name =>
      val columnNameOpt: Option[String] = name match {
        case CN1(n) => Some(n)
        case CN2(n) => Some(n)
        case _ => None
      }
      columnNameOpt.foreach { columnName =>
        val exists = fieldNames.contains(columnName)
        if !exists then {
          scribe.info(s"Removing unused index: $name")
          executeUpdate(s"DROP INDEX IF EXISTS $name", tx)
        }
      }
    }
    connection.commit()

    // Add/Remove columns
    val existingColumns = columns(connection)
    // Drop columns
    existingColumns.foreach { name =>
      if !fieldNames.contains(name.toLowerCase) then {
        scribe.info(s"Removing column $fqn.$name (existing: ${existingColumns.mkString(", ")}, expected: ${fieldNames.mkString(", ")}).")
        executeUpdate(s"ALTER TABLE $fqn DROP COLUMN ${SqlIdent.quote(name)}", tx)
      }
    }
    // Add columns
    fields.foreach { field =>
      val name = field.name
      if !existingColumns.contains(name.toLowerCase) then {
        addColumn(field, tx)
      }
    }

    // Add indexes
    fields.foreach {
      case index: UniqueIndex[Doc, _] if index.name == "_id" => // Ignore _id
      case index: UniqueIndex[Doc, _] => executeUpdate(createUniqueIndexSQL(index), tx)
      case index: Indexed[Doc, _] => executeUpdate(createIndexSQL(index), tx)
      case _: Field[Doc, _] => // Nothing to do
    }

    // Add composite indexes
    model.compositeIndexes.foreach { compositeIndex =>
      executeUpdate(createCompositeIndexSQL(compositeIndex), tx)
    }
  }

  protected def createUniqueIndexSQL(index: UniqueIndex[Doc, _]): String =
    s"CREATE UNIQUE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${index.name}_idx")} ON $fqn(${SqlIdent.quote(index.name)})"

  protected def createIndexSQL(index: Indexed[Doc, _]): String =
    s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${index.name}_idx")} ON $fqn(${SqlIdent.quote(index.name)})"

  protected def createCompositeIndexSQL(compositeIndex: CompositeIndex[Doc]): String = {
    val include = compositeIndex.include match {
      case Nil => ""
      case list => s" INCLUDE(${list.map(f => SqlIdent.quote(f.name)).mkString(", ")})"
    }
    s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${compositeIndex.name}_idx")} ON $fqn(${compositeIndex.fields.map(f => SqlIdent.quote(f.name)).mkString(", ")})$include"
  }

  protected def tables(connection: Connection): Set[String]

  private def columns(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM $fqn LIMIT 1")
    try {
      val rs = ps.executeQuery()
      val meta = rs.getMetaData
      (1 to meta.getColumnCount).map { index =>
        meta.getColumnName(index).toLowerCase
      }.toSet
    } finally {
      ps.close()
    }
  }

  protected def indexes(connection: Connection): Set[String] = {
    val meta = connection.getMetaData
    val schema = if supportsSchemas then {
      lightDB.name
    } else {
      null
    }
    val rs = meta.getIndexInfo(null, schema, name, false, false)
    try {
      var set = Set.empty[String]
      while rs.next() do {
        val indexName = rs.getString("INDEX_NAME")
        val indexType = rs.getShort("TYPE")
        val isStatistic = indexType == DatabaseMetaData.tableIndexStatistic
        if indexName != null && !isStatistic then {
          set += indexName.toLowerCase
        }
      }
      set
    } finally {
      rs.close()
    }
  }

  protected def field2Value(field: Field[Doc, _]): String = "?"

  protected def insertPrefix: String = "INSERT"

  protected def upsertPrefix: String = "INSERT OR REPLACE"

  protected def createInsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$insertPrefix INTO $fqn(${fields.map(f => SqlIdent.quote(f.name)).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  protected def createUpsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$upsertPrefix INTO $fqn(${fields.map(f => SqlIdent.quote(f.name)).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  private[sql] lazy val insertSQL: String = createInsertSQL()
  private[sql] lazy val upsertSQL: String = createUpsertSQL()

  protected def executeUpdate(sql: String, tx: TX): Unit = {
    val state = tx.state
    val connection = state.connectionManager.getConnection(state)
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Failed to execute update: $sql", t)
    } finally {
      s.close()
    }
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(connectionManager.release())
}