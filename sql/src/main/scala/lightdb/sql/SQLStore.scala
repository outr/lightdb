package lightdb.sql

import fabric.define.DefType
import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.sql.connect.ConnectionManager
import lightdb.store._
import lightdb.store.prefix.PrefixScanningStore
import rapid.Task

import java.nio.file.Path
import java.sql.{Connection, DatabaseMetaData}
import scala.language.implicitConversions

abstract class SQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, lightDB, storeManager) with PrefixScanningStore[Doc, Model] {
  override type TX <: SQLStoreTransaction[Doc, Model]

  override def supportsNativeExistsChild: Boolean = false

  def supportsSchemas: Boolean = false

  lazy val fqn: String = if (supportsSchemas) {
    s"${lightDB.name}.$name"
  } else {
    name
  }

  protected def connectionManager: ConnectionManager

  override protected def initialize(): Task[Unit] = super.initialize().next(Task.next {
    transaction(initTransaction)
  })

  protected def createSchema(tx: TX): Unit = {
    executeUpdate(s"CREATE SCHEMA IF NOT EXISTS ${lightDB.name}", tx)
  }

  protected def createTable(tx: TX): Unit = {
    val entries = fields.collect {
      case field if !field.rw.definition.className.contains("lightdb.spatial.GeoPoint") =>
        if (field == model._id) {
          "_id VARCHAR NOT NULL PRIMARY KEY"
        } else {
          val t = def2Type(field.name, field.rw.definition)
          s"${field.name} $t"
        }
    }.mkString(", ")
    executeUpdate(s"CREATE TABLE IF NOT EXISTS $fqn($entries)", tx)
  }

  protected def def2Type(name: String, d: DefType): String = d match {
    case DefType.Str | DefType.Json | DefType.Obj(_, _) | DefType.Arr(_) | DefType.Poly(_, _) | DefType.Enum(_, _) =>
      "VARCHAR"
    case DefType.Int => "BIGINT"
    case DefType.Bool => "TINYINT"
    case DefType.Dec => "DOUBLE"
    case DefType.Opt(d) => def2Type(name, d)
    case d => throw new UnsupportedOperationException(s"$name has an unsupported type: $d")
  }

  protected def addColumn(field: Field[Doc, _], tx: TX): Unit = {
    scribe.info(s"Adding column $fqn.${field.name}")
    executeUpdate(s"ALTER TABLE $fqn ADD COLUMN ${field.name} ${def2Type(field.name, field.rw.definition)}", tx)
  }

  protected def initConnection(connection: Connection): Unit = {}

  protected def initTransaction(tx: TX): Task[Unit] = Task {
    connectionManager.active()
    val connection = tx.state.connectionManager.getConnection(tx.state)
    initConnection(connection)
    if (supportsSchemas) {
      createSchema(tx)
    }
    val existingTables = tables(connection)
    if (!existingTables.contains(name.toLowerCase)) {
      createTable(tx)
    }

    val fieldNames = fields.map(_.name.toLowerCase).toSet

    // Remove bad indexes
    val CN1 = """.+_(.+)_idx""".r
    val CN2 = """(.+)_idx""".r
    val existingIndexes = indexes(connection)
    existingIndexes.filterNot(_.contains("autoindex")).foreach { name =>
      val columnName = name match {
        case CN1(n) => n
        case CN2(n) => n
      }
      val exists = fieldNames.contains(columnName)
      if (!exists) {
        scribe.info(s"Removing unused index: $name")
        executeUpdate(s"DROP INDEX IF EXISTS $name", tx)
      }
    }
    connection.commit()

    // Add/Remove columns
    val existingColumns = columns(connection)
    // Drop columns
    existingColumns.foreach { name =>
      if (!fieldNames.contains(name.toLowerCase)) {
        scribe.info(s"Removing column $fqn.$name (existing: ${existingColumns.mkString(", ")}, expected: ${fieldNames.mkString(", ")}).")
        executeUpdate(s"ALTER TABLE $fqn DROP COLUMN $name", tx)
      }
    }
    // Add columns
    fields.foreach { field =>
      val name = field.name
      if (!existingColumns.contains(name.toLowerCase)) {
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
    s"CREATE UNIQUE INDEX IF NOT EXISTS ${name}_${index.name}_idx ON $fqn(${index.name})"

  protected def createIndexSQL(index: Indexed[Doc, _]): String =
    s"CREATE INDEX IF NOT EXISTS ${name}_${index.name}_idx ON $fqn(${index.name})"

  protected def createCompositeIndexSQL(compositeIndex: CompositeIndex[Doc]): String = {
    val include = compositeIndex.include match {
      case Nil => ""
      case list => s" INCLUDE(${list.map(_.name).mkString(", ")})"
    }
    s"CREATE INDEX IF NOT EXISTS ${name}_${compositeIndex.name}_idx ON $fqn(${compositeIndex.fields.map(_.name).mkString(", ")})$include"
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
    val schema = if (supportsSchemas) {
      lightDB.name
    } else {
      null
    }
    val rs = meta.getIndexInfo(null, schema, name, false, false)
    try {
      var set = Set.empty[String]
      while (rs.next()) {
        val indexName = rs.getString("INDEX_NAME")
        val indexType = rs.getShort("TYPE")
        val isStatistic = indexType == DatabaseMetaData.tableIndexStatistic
        if (indexName != null && !isStatistic) {
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
    s"$insertPrefix INTO $fqn(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  protected def createUpsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$upsertPrefix INTO $fqn(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
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