package lightdb.sql

import fabric._
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.LightDB
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.spatial.{Geo, Spatial}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.store.{CollectionManager, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.sqlite.Collation
import rapid._

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.util.regex.Pattern

class SQLiteStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val connectionManager: ConnectionManager,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = SQLiteTransaction[Doc, Model]

  override protected def initConnection(connection: Connection): Unit = {
    super.initConnection(connection)
    // SQLite requires switching journal_mode outside of an active transaction.
    // Our default SQLConfig uses autoCommit=false, which can cause SQLite JDBC to
    // treat us as "in transaction" already. Temporarily enable auto-commit to
    // ensure journal_mode can be set deterministically.
    val previousAutoCommit = try connection.getAutoCommit catch {
      case _: Throwable => true
    }
    if !previousAutoCommit then {
      try {
        // End any implicit transaction context before setting journal_mode.
        connection.setAutoCommit(true)
      } catch {
        case _: Throwable => // best-effort
      }
    }
    val s = connection.createStatement()
    try {
      // Apply tuning pragmas first.
      SQLiteStore.Pragmas.foreach {
        case (k, v) =>
          try s.execute(s"PRAGMA $k=$v;")
          catch { case _: Throwable => () }
      }

      s.execute("PRAGMA journal_mode=WAL;")
    } catch {
      case t: Throwable =>
        scribe.warn(s"Unable to enable SQLite WAL (journal_mode=WAL) for store '$name'. Continuing without WAL.", t)
    } finally {
      try s.close() catch { case _: Throwable => () }
      if !previousAutoCommit then {
        try connection.setAutoCommit(false) catch { case _: Throwable => () }
      }
    }
  }

  private def createFTS(tx: TX): Unit = {
    val tokenized = fields.collect { case t: lightdb.field.Field.Tokenized[Doc] => t }
    if tokenized.isEmpty then return

    val ftsTable = s"${this.name}__fts"
    val cols = tokenized.map(_.name)
    // contentless fts: we manage inserts via triggers
    val colSql = (List("_id UNINDEXED") ::: cols).mkString(", ")
    executeUpdate(s"CREATE VIRTUAL TABLE IF NOT EXISTS $ftsTable USING fts5($colSql)", tx)

    // Keep FTS table in sync with base table.
    val insertCols = ("_id" :: cols).mkString(", ")
    val newCols = ("new._id" :: cols.map(c => s"new.$c")).mkString(", ")

    executeUpdate(
      s"""CREATE TRIGGER IF NOT EXISTS ${this.name}__fts_ai AFTER INSERT ON ${this.name} BEGIN
         |  INSERT INTO $ftsTable($insertCols) VALUES($newCols);
         |END;""".stripMargin, tx
    )
    executeUpdate(
      s"""CREATE TRIGGER IF NOT EXISTS ${this.name}__fts_ad AFTER DELETE ON ${this.name} BEGIN
         |  DELETE FROM $ftsTable WHERE _id = old._id;
         |END;""".stripMargin, tx
    )
    executeUpdate(
      s"""CREATE TRIGGER IF NOT EXISTS ${this.name}__fts_au AFTER UPDATE ON ${this.name} BEGIN
         |  DELETE FROM $ftsTable WHERE _id = old._id;
         |  INSERT INTO $ftsTable($insertCols) VALUES($newCols);
         |END;""".stripMargin, tx
    )
  }

  private def createMultiValueIndexes(tx: TX): Unit = {
    // Only for array-like indexed fields (List/Set/etc.) where the stored representation is JSON array.
    val arrayIndexed = fields.collect {
      case f if f.indexed && f.isArr => f
    }
    arrayIndexed.foreach { f =>
      val mvTable = s"${this.name}__mv__${f.name}"
      executeUpdate(s"CREATE TABLE IF NOT EXISTS $mvTable(owner_id TEXT NOT NULL, value TEXT NOT NULL)", tx)
      executeUpdate(s"CREATE INDEX IF NOT EXISTS ${mvTable}__value_idx ON $mvTable(value)", tx)
      executeUpdate(s"CREATE INDEX IF NOT EXISTS ${mvTable}__owner_value_idx ON $mvTable(owner_id, value)", tx)

      // Sync table via triggers using json_each(new.<field>).
      executeUpdate(
        s"""CREATE TRIGGER IF NOT EXISTS ${mvTable}__ai AFTER INSERT ON ${this.name} BEGIN
           |  DELETE FROM $mvTable WHERE owner_id = new._id;
           |  INSERT INTO $mvTable(owner_id, value)
           |    SELECT new._id, CAST(value AS TEXT) FROM json_each(new.${f.name});
           |END;""".stripMargin, tx
      )
      executeUpdate(
        s"""CREATE TRIGGER IF NOT EXISTS ${mvTable}__au AFTER UPDATE ON ${this.name} BEGIN
           |  DELETE FROM $mvTable WHERE owner_id = new._id;
           |  INSERT INTO $mvTable(owner_id, value)
           |    SELECT new._id, CAST(value AS TEXT) FROM json_each(new.${f.name});
           |END;""".stripMargin, tx
      )
      executeUpdate(
        s"""CREATE TRIGGER IF NOT EXISTS ${mvTable}__ad AFTER DELETE ON ${this.name} BEGIN
           |  DELETE FROM $mvTable WHERE owner_id = old._id;
           |END;""".stripMargin, tx
      )
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    SQLiteTransaction(this, state, parent)
  }

  override protected def initTransaction(tx: TX): Task[Unit] = super.initTransaction(tx).map { _ =>
    // Schema is now ensured; build optional auxiliary structures.
    if SQLiteStore.EnableFTS then {
      createFTS(tx)
    }
    if SQLiteStore.EnableMultiValueIndexes then {
      createMultiValueIndexes(tx)
    }

    val c = tx.state.connectionManager.getConnection(tx.state)
    if hasSpatial.sync() then {
      scribe.info(s"$name has spatial features. Enabling...")
      org.sqlite.Function.create(c, "DISTANCE", new org.sqlite.Function() {
        override def xFunc(): Unit = {
          def s(index: Int): List[Geo] = Option(value_text(index))
            .map(s => JsonParser(s))
            .map {
              case Arr(vector, _) => vector.toList.map(_.as[Geo])
              case json => List(json.as[Geo])
            }
            .getOrElse(Nil)
          val shapes1 = s(0)
          val shapes2 = s(1)
          val distances = shapes1.flatMap { geo1 =>
            shapes2.map { geo2 =>
              Spatial.distance(geo1, geo2)
            }
          }
          result(JsonFormatter.Compact(distances.json))
        }
      })
      org.sqlite.Function.create(c, "DISTANCE_LESS_THAN", new org.sqlite.Function() {
        override def xFunc(): Unit = {
          val distances = Option(value_text(0))
            .map(s => JsonParser(s).as[List[Distance]])
            .getOrElse(Nil)
          val value = value_text(1).toDouble
          val b = distances.exists(d => d.valueInMeters <= value)
          result(if b then 1 else 0)
        }
      })
      org.sqlite.Collation.create(c, "DISTANCE_SORT_ASCENDING", new Collation() {
        override def xCompare(str1: String, str2: String): Int = {
          val min1 = JsonParser(str1).as[List[Double]].min
          val min2 = JsonParser(str2).as[List[Double]].min
          min1.compareTo(min2)
        }
      })
      org.sqlite.Collation.create(c, "DISTANCE_SORT_DESCENDING", new Collation() {
        override def xCompare(str1: String, str2: String): Int = {
          val min1 = JsonParser(str1).as[List[Double]].min
          val min2 = JsonParser(str2).as[List[Double]].min
          min2.compareTo(min1)
        }
      })
    }
    org.sqlite.Function.create(c, "REGEXP", new org.sqlite.Function() {
      override def xFunc(): Unit = {
        val expression = value_text(0)
        val value = Option(value_text(1)).getOrElse("")
        val pattern = Pattern.compile(expression)
        result(if pattern.matcher(value).find() then 1 else 0)
      }
    })
  }

  override protected def tables(connection: Connection): Set[String] = SQLiteStore.tables(connection)
}

object SQLiteStore extends SQLCollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = SQLiteStore[Doc, Model]

  /**
   * SQLite tuning knobs (applied via PRAGMA on connection init).
   * These default to conservative, concurrency-friendly values for WAL.
   */
  var Pragmas: Map[String, String] = Map(
    "busy_timeout" -> "5000",
    // WAL is generally best with NORMAL synchronous for throughput; change to FULL if you need max durability.
    "synchronous" -> "NORMAL",
    // Larger cache helps read-heavy workloads. Negative means KiB.
    "cache_size" -> "-200000",
    // Try to checkpoint periodically to keep WAL from growing unbounded.
    "wal_autocheckpoint" -> "2000",
    "temp_store" -> "MEMORY"
  )

  /**
   * Enables FTS5 tables for tokenized fields. Requires SQLite compiled with FTS5 (common in modern builds).
   */
  var EnableFTS: Boolean = true

  /**
   * Enables auxiliary index tables for array-like fields (List/Set) to make membership filters index-backed.
   * Requires SQLite JSON1 (`json_each`) to be available.
   */
  var EnableMultiValueIndexes: Boolean = true

  def singleConnectionManager(file: Option[Path]): ConnectionManager = {
    file match {
      case Some(f) =>
        Files.createDirectories(f)
        val file = f.toFile.getCanonicalPath
        SingleConnectionManager(SQLConfig(
          jdbcUrl = s"jdbc:sqlite:$file/db.sqlite"
        ))
      case None =>
        // In-memory SQLite must not append a file path suffix.
        SingleConnectionManager(SQLConfig(
          jdbcUrl = "jdbc:sqlite::memory:"
        ))
    }
  }

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                               path: Option[Path],
                                                               model: Model,
                                                               storeMode: StoreMode[Doc, Model],
                                                               db: LightDB): SQLiteStore[Doc, Model] = {
    new SQLiteStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      connectionManager = singleConnectionManager(path),
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): SQLiteStore[Doc, Model] = {
    val n = name.substring(name.indexOf('/') + 1)
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) =>
        new SQLiteStore[Doc, Model](
          name = n,
          path = path,
          model = model,
          connectionManager = sqlDB.connectionManager,
          storeMode = storeMode,
          lightDB = db,
          storeManager = this
        )
      case None => apply[Doc, Model](n, path, model, storeMode, db)
    }
  }

  private def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT name FROM sqlite_master WHERE type = 'table';")
    try {
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while rs.next() do {
          set += rs.getString("name").toLowerCase
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
