package lightdb.h2

import lightdb.{LightDB, Unique}
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.{SQLDatabase, SQLStore}
import lightdb.sql.connect.{ConnectionManager, HikariConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.store.{Store, StoreManager, StoreMode}

import java.io.File
import java.nio.file.Path
import java.sql.Connection

// TODO: Look into http://www.h2gis.org/docs/1.5.0/quickstart/ for spatial support
class H2Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val connectionManager: ConnectionManager,
                                                                 val connectionShared: Boolean,
                                                                 val storeMode: StoreMode) extends SQLStore[Doc, Model] {
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
    jdbcUrl = s"jdbc:h2:${file.map(_.toFile.getCanonicalPath).getOrElse(s"test:${Unique()}")};NON_KEYWORDS=VALUE,USER"
  )

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](file: Option[Path],
                                                               storeMode: StoreMode): H2Store[Doc, Model] =
    new H2Store[Doc, Model](
      connectionManager = SingleConnectionManager(config(file)),
      connectionShared = false,
      storeMode = storeMode
    )

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) => new H2Store[Doc, Model](
        connectionManager = sqlDB.connectionManager,
        connectionShared = true,
        storeMode
      )
      case None => apply[Doc, Model](db.directory.map(_.resolve(s"$name.h2")), storeMode)
    }
  }
}