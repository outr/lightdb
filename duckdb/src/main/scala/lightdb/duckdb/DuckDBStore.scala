package lightdb.duckdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, DBCPConnectionManager, SQLConfig}
import lightdb.sql.{SQLDatabase, SQLState, SQLStore, SqlIdent}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.Task

import java.nio.file.Path
import java.sql.Connection

class DuckDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val connectionManager: ConnectionManager,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = DuckDBTransaction[Doc, Model]

  override protected def createUpsertSQL(): String = {
    val cols = fields.map(f => SqlIdent.quote(f.name))
    val values = cols.map(_ => "?")
    val assignments = fields.filterNot(_ == model._id).map { f =>
      val q = SqlIdent.quote(f.name)
      s"$q=excluded.$q"
    }
    val idCol = SqlIdent.quote(model._id.name)
    val update = if assignments.nonEmpty then assignments.mkString(", ") else s"$idCol=$idCol"
    s"INSERT INTO $fqn(${cols.mkString(", ")}) VALUES(${values.mkString(", ")}) ON CONFLICT($idCol) DO UPDATE SET $update"
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[DuckDBTransaction[Doc, Model]] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    DuckDBTransaction(this, state, parent, writeHandlerFactory)
  }

  /**
   * Loads a CSV file into this store's table, natively: DuckDB reads the file in parallel and inserts straight from it (see
   * [[CsvImportOptions]]), so no row passes through the JVM. In `tx`, so a failure (a missing column, a cancel, a malformed row) leaves
   * the table as it was.
   *
   * Only the columns [[CsvImportOptions.columns]] name, the row number field and `_id` are written: any other field of the model is
   * left NULL, so a model that is read back as a document (`JsonDocumentModel`'s `json` column) has nothing to read it from; query it
   * by field (`.value(...)`, `.json(...)`, filters, sorts, counts) instead. A model that mixes in [[lightdb.sql.KeylessModel]] has no
   * primary key, which makes the load and the file markedly smaller.
   */
  def importCsv(csv: Path, options: CsvImportOptions, control: CsvImportControl = new CsvImportControl)(tx: TX): Task[CsvImportResult] = Task {
    val started = System.currentTimeMillis()
    val connection = tx.state.openConnection()
    val source = CsvImport.readCsv(csv, options)
    val known = fields.map(_.name).toSet
    // the file's own column names, as DuckDB reads them (column0, column1 ... when there is no header)
    val available: Vector[String] = {
      val s = connection.createStatement()
      try {
        val rs = s.executeQuery(s"DESCRIBE SELECT * FROM $source")
        val names = Vector.newBuilder[String]
        while rs.next() do names += rs.getString("column_name")
        names.result()
      } finally s.close()
    }
    def resolve(c: CsvColumn): String = c.source match {
      case CsvColumn.Source.Place(n) =>
        if n >= 1 && n <= available.size then available(n - 1)
        else throw new IllegalArgumentException(s"The CSV has ${available.size} columns, so there is no column $n (for ${c.field}).")
      case CsvColumn.Source.Heading(h) =>
        available.find(_ == h).orElse(available.find(_.equalsIgnoreCase(h.trim)))
          .getOrElse(throw new IllegalArgumentException(s"The CSV has no column headed \"$h\" (for ${c.field}); it has: ${available.mkString(", ")}."))
    }
    options.columns.foreach { c =>
      if !known.contains(c.field) then throw new IllegalArgumentException(s"${name} has no field ${c.field}; it has: ${known.toList.sorted.mkString(", ")}.")
    }
    options.rowNumberField.foreach { f =>
      if !known.contains(f) then throw new IllegalArgumentException(s"${name} has no field $f for the row numbers.")
    }
    val rowNumber = "\"__row\""
    val targets = List(SqlIdent.quote("_id")) ++ options.rowNumberField.map(SqlIdent.quote) ++ options.columns.map(c => SqlIdent.quote(c.field))
    val values = List(if options.idFromRowNumber then s"CAST($rowNumber AS VARCHAR)" else "CAST(uuid() AS VARCHAR)") ++
      options.rowNumberField.map(_ => rowNumber) ++ options.columns.map { c =>
      val column = SqlIdent.quote(resolve(c))
      c.tryCast.fold(column)(t => s"TRY_CAST($column AS ${CsvImport.typeName(t)})")
    }
    if options.replace then {
      val s = connection.createStatement()
      try s.executeUpdate(s"DELETE FROM $fqn") finally s.close()
    }
    val statement = connection.createStatement()
    val loaded =
      try {
        control.running(statement)
        statement.executeLargeUpdate(s"INSERT INTO $fqn(${targets.mkString(", ")}) SELECT ${values.mkString(", ")} FROM (SELECT row_number() OVER () AS $rowNumber, * FROM $source)")
      } catch {
        case t: Throwable if control.isCancelled =>
          val cancelled = new java.util.concurrent.CancellationException(s"The import of ${csv.getFileName} was cancelled.")
          cancelled.initCause(t)
          throw cancelled
      } finally statement.close()
    CsvImportResult(loaded, System.currentTimeMillis() - started)
  }

  // TODO: Use DuckDB's Appender for better performance
  /*override def insert(doc: Doc)(transaction: Transaction[Doc]): Unit = {
    fields.zipWithIndex.foreach {
      case (field, index) =>
        val c = connectionManager.getConnection.asInstanceOf[DuckDBConnection]
        val a =
    }
  }*/

  override protected def indexes(connection: Connection): Set[String] = {
    val sql = if supportsSchemas then {
      s"SELECT LOWER(index_name) AS name FROM duckdb_indexes() WHERE LOWER(schema_name) = LOWER(?) AND LOWER(table_name) = LOWER(?)"
    } else {
      s"SELECT LOWER(index_name) AS name FROM duckdb_indexes() WHERE LOWER(table_name) = LOWER(?)"
    }
    val ps = connection.prepareStatement(sql)
    try {
      if supportsSchemas then {
        ps.setString(1, lightDB.name)
        ps.setString(2, name)
      } else {
        ps.setString(1, name)
      }
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while rs.next() do {
          set += rs.getString("name")
        }
        set
      } finally {
        rs.close()
      }
    } finally {
      ps.close()
    }
  }

  override protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'")
    try {
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while rs.next() do {
          set += rs.getString("table_name").toLowerCase
        }
        set
      } finally {
        rs.close()
      }
    } finally {
      ps.close()
    }
  }
}

object DuckDBStore extends lightdb.sql.SQLCollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = DuckDBStore[Doc, Model]

  private def pooledConnectionManager(file: Option[Path]): ConnectionManager = {
    val path = file match {
      case Some(f) =>
        val file = f.toFile
        Option(file.getParentFile).foreach(_.mkdirs())
        file.getCanonicalPath
      case None => ""
    }
    // DuckDB JDBC cannot have multiple concurrent operations on the same connection;
    // use a small pool so each transaction gets its own connection.
    DBCPConnectionManager(SQLConfig(
      jdbcUrl = s"jdbc:duckdb:$path",
      maximumPoolSize = Some(8),
      autoCommit = false
    ))
  }

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                               path: Option[Path],
                                                               model: Model,
                                                               storeMode: StoreMode[Doc, Model],
                                                               db: LightDB): DuckDBStore[Doc, Model] = {
    new DuckDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      connectionManager = pooledConnectionManager(path),
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) => new DuckDBStore[Doc, Model](
        name = name,
        path = path,
        model = model,
        connectionManager = sqlDB.connectionManager,
        storeMode = storeMode,
        lightDB = db,
        storeManager = this
      )
      case None => apply[Doc, Model](name, path, model, storeMode, db)
    }
  }
}