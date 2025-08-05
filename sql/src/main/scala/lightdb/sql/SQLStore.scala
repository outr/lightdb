package lightdb.sql

import fabric.define.DefType
import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.sql.connect.ConnectionManager
import lightdb.store._
import rapid.Task

import java.nio.file.Path
import java.sql.Connection
import scala.language.implicitConversions

abstract class SQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX <: SQLStoreTransaction[Doc, Model]

  def booleanAsNumber: Boolean = true

  protected def connectionManager: ConnectionManager

  override protected def initialize(): Task[Unit] = super.initialize().next(Task.next {
    transaction(initTransaction)
  })

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
    executeUpdate(s"CREATE TABLE IF NOT EXISTS $name($entries)", tx)
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
    scribe.info(s"Adding column $name.${field.name}")
    executeUpdate(s"ALTER TABLE $name ADD COLUMN ${field.name} ${def2Type(field.name, field.rw.definition)}", tx)
  }

  protected def initTransaction(tx: TX): Task[Unit] = Task {
    connectionManager.active()
    val connection = tx.state.connectionManager.getConnection(tx.state)
    val existingTables = tables(connection)
    if (!existingTables.contains(name.toLowerCase)) {
      createTable(tx)
    }

    // Add/Remove columns
    val existingColumns = columns(connection)
    val fieldNames = fields.map(_.name.toLowerCase).toSet
    // Drop columns
    existingColumns.foreach { name =>
      if (!fieldNames.contains(name.toLowerCase)) {
        scribe.info(s"Removing column ${this.name}.$name (existing: ${existingColumns.mkString(", ")}, expected: ${fieldNames.mkString(", ")}).")
        executeUpdate(s"ALTER TABLE ${this.name} DROP COLUMN $name", tx)
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
      case index: UniqueIndex[Doc, _] =>
        executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${index.name}_idx ON $name(${index.name})", tx)
      case index: Indexed[Doc, _] =>
        executeUpdate(s"CREATE INDEX IF NOT EXISTS ${index.name}_idx ON $name(${index.name})", tx)
      case _: Field[Doc, _] => // Nothing to do
    }
  }

  protected def tables(connection: Connection): Set[String]

  private def columns(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM $name LIMIT 1")
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

  protected def field2Value(field: Field[Doc, _]): String = "?"

  protected def insertPrefix: String = "INSERT"

  protected def upsertPrefix: String = "INSERT OR REPLACE"

  protected def createInsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$insertPrefix INTO $name(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  protected def createUpsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$upsertPrefix INTO $name(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
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