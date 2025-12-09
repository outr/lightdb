package lightdb.h2

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLState, SQLStore}
import lightdb.store.{CollectionManager, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.{Task, Unique}

import java.nio.file.Path
import java.sql.Connection

class H2Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                 path: Option[Path],
                                                                 model: Model,
                                                                 val connectionManager: ConnectionManager,
                                                                 val storeMode: StoreMode[Doc, Model],
                                                                 lightDB: LightDB,
                                                                 storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = H2Transaction[Doc, Model]

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    H2Transaction(this, state, parent)
  }

  override protected def upsertPrefix: String = "MERGE"

  override protected def initTransaction(tx: TX): Task[Unit] = super.initTransaction(tx).flatMap { _ =>
    Task {
      val c = tx.state.connectionManager.getConnection(tx.state)
      val s = c.createStatement()
      try {
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
        while (rs.next()) {
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

object H2Store extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = H2Store[Doc, Model]

  def config(file: Option[Path]): SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:h2:${file.map(_.toFile.getCanonicalPath).map(p => s"file:$p").getOrElse(s"test:${Unique.sync()}")};NON_KEYWORDS=VALUE,USER,SEARCH"
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
