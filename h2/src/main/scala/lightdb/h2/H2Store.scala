package lightdb.h2

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLState, SQLStore}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.{Task, Unique}

import java.nio.file.Path
import java.sql.Connection
import scala.util.Try

class H2Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                 path: Option[Path],
                                                                 model: Model,
                                                                 val connectionManager: ConnectionManager,
                                                                 val storeMode: StoreMode[Doc, Model],
                                                                 lightDB: LightDB,
                                                                 storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = H2Transaction[Doc, Model]

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    H2Transaction(this, state, parent, writeHandlerFactory)
  }

  override protected def upsertPrefix: String = "MERGE"

  override protected def initConnection(connection: Connection): Unit = {
    super.initConnection(connection)
    val s = connection.createStatement()
    try {
      H2Store.Settings.foreach { sql =>
        Try(s.execute(sql))
      }
    } finally {
      s.close()
    }
  }

  override protected def initTransaction(tx: TX): Task[Unit] = super.initTransaction(tx).flatMap { _ =>
    Task {
      val c = tx.state.connectionManager.getConnection(tx.state)
      val s = c.createStatement()
      try {
        // Apply optional SQL features (multi-valued indexing helpers)
        if H2Store.EnableMultiValueIndexes then {
          // Create auxiliary tables + triggers for array-like indexed fields.
          val arrayIndexed = fields.collect { case f if f.indexed && f.isArr => f }
          arrayIndexed.foreach { f =>
            val mvTable = s"${name}__mv__${f.name}"
            s.execute(s"CREATE TABLE IF NOT EXISTS $mvTable(owner_id VARCHAR NOT NULL, value VARCHAR NOT NULL)")
            s.execute(s"CREATE INDEX IF NOT EXISTS ${mvTable}__value_idx ON $mvTable(value)")
            s.execute(s"CREATE INDEX IF NOT EXISTS ${mvTable}__owner_value_idx ON $mvTable(owner_id, value)")

            // Use a Java trigger (H2 2.x does not support BEGIN ATOMIC in CREATE TRIGGER).
            def createTrigger(triggerName: String, timing: String, event: String): Unit = {
              // H2 does not support IF NOT EXISTS for CREATE TRIGGER reliably across versions.
              val sql = s"""CREATE TRIGGER $triggerName $timing $event ON $name
                           |FOR EACH ROW CALL "lightdb.h2.H2MultiValueTrigger"""".stripMargin
              try s.execute(sql) catch { case _: Throwable => () } // ignore if already exists
            }

            createTrigger(s"${mvTable}__ai", "AFTER", "INSERT")
            createTrigger(s"${mvTable}__au", "AFTER", "UPDATE")
            createTrigger(s"${mvTable}__ad", "AFTER", "DELETE")
          }
        }

        s.execute("""CREATE ALIAS IF NOT EXISTS GEO_DISTANCE_JSON FOR "lightdb.h2.H2SpatialFunctions.distanceJson"""")
        s.execute("""CREATE ALIAS IF NOT EXISTS GEO_DISTANCE_MIN FOR "lightdb.h2.H2SpatialFunctions.distanceMin"""")
      } finally {
        s.close()
      }
    }
  }

  override protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'PUBLIC';")
    try {
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while rs.next() do {
          set += rs.getString("TABLE_NAME").toLowerCase
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

object H2Store extends lightdb.sql.SQLCollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = H2Store[Doc, Model]

  /** H2 settings applied per-connection (best-effort). */
  var Settings: List[String] = List(
    "SET LOCK_TIMEOUT 10000",
    "SET QUERY_TIMEOUT 0"
  )

  /**
   * H2 fulltext is not enabled by default here because integrating a reliable, indexed, scored
   * best-match implementation across H2 versions requires more specialized plumbing.
   *
   * (We still support BestMatch scoring heuristically in SQL translation.)
   */
  var EnableFTS: Boolean = false
  var EnableMultiValueIndexes: Boolean = true

  def config(file: Option[Path]): SQLConfig = SQLConfig(
    jdbcUrl = file match {
      case Some(p) =>
        val abs = p.toFile.getCanonicalPath
        s"jdbc:h2:file:$abs;NON_KEYWORDS=VALUE,USER,SEARCH"
      case None =>
        // Use a true in-memory database (H2 2.x disallows implicit relative paths like "test:xyz")
        s"jdbc:h2:mem:${Unique.sync()};DB_CLOSE_DELAY=-1;NON_KEYWORDS=VALUE,USER,SEARCH"
    }
  )

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                               path: Option[Path],
                                                               model: Model,
                                                               storeMode: StoreMode[Doc, Model],
                                                               db: LightDB): H2Store[Doc, Model] =
    new H2Store[Doc, Model](
      name = name,
      path = path,
      model = model,
      connectionManager = SingleConnectionManager(config(path)),
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): H2Store[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) => new H2Store[Doc, Model](
        name = name,
        path = path,
        model = model,
        connectionManager = sqlDB.connectionManager,
        storeMode,
        lightDB = db,
        this
      )
      case None => apply[Doc, Model](name, path, model, storeMode, db)
    }
  }
}
