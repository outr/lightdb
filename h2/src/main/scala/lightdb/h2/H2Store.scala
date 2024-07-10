package lightdb.h2

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLStore
import lightdb.sql.connect.{ConnectionManager, HikariConnectionManager, SQLConfig}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.util.Unique

import java.nio.file.Path
import java.sql.Connection

class H2Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](file: Option[Path], val storeMode: StoreMode) extends SQLStore[Doc, Model] {
  override protected lazy val connectionManager: ConnectionManager[Doc] = HikariConnectionManager(SQLConfig(
    jdbcUrl = s"jdbc:h2:${file.map(_.toFile.getCanonicalPath).getOrElse(s"test:${Unique()}")}"
  ))

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
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] =
    new H2Store[Doc, Model](db.directory.map(_.resolve(s"$name.h2")), storeMode)
}