package lightdb.h2

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLStore}
import lightdb.store.{Store, StoreManager, StoreMode}
import rapid.Unique

import java.nio.file.Path
import java.sql.Connection

// TODO: Look into http://www.h2gis.org/docs/1.5.0/quickstart/ for spatial support
class H2Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                 model: Model,
                                                                 val connectionManager: ConnectionManager,
                                                                 val storeMode: StoreMode[Doc, Model],
                                                                 storeManager: StoreManager) extends SQLStore[Doc, Model](name, model, storeManager) {
  override protected def upsertPrefix: String = "MERGE"

  protected def tables(connection: Connection): Set[String] = {
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

object H2Store extends StoreManager {
  def config(file: Option[Path]): SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:h2:${file.map(_.toFile.getCanonicalPath).map(p => s"file:$p").getOrElse(s"test:${Unique.sync()}")};NON_KEYWORDS=VALUE,USER,SEARCH"
  )

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                               model: Model,
                                                               file: Option[Path],
                                                               storeMode: StoreMode[Doc, Model]): H2Store[Doc, Model] =
    new H2Store[Doc, Model](
      name = name,
      model = model,
      connectionManager = SingleConnectionManager(config(file)),
      storeMode = storeMode,
      storeManager = this
    )

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) => new H2Store[Doc, Model](
        name = name,
        model = model,
        connectionManager = sqlDB.connectionManager,
        storeMode,
        this
      )
      case None => apply[Doc, Model](name, model, db.directory.map(_.resolve(s"$name.h2")), storeMode)
    }
  }
}