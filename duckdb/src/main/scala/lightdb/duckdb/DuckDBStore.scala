package lightdb.duckdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLStore}
import lightdb.store.{CollectionManager, StoreManager, StoreMode}

import java.nio.file.Path
import java.sql.Connection

class DuckDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val connectionManager: ConnectionManager,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  // TODO: Use DuckDB's Appender for better performance
  /*override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    fields.zipWithIndex.foreach {
      case (field, index) =>
        val c = connectionManager.getConnection.asInstanceOf[DuckDBConnection]
        val a =
    }
  }*/

  override protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'")
    try {
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while (rs.next()) {
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

object DuckDBStore extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = DuckDBStore[Doc, Model]

  def singleConnectionManager(file: Option[Path]): ConnectionManager = {
    val path = file match {
      case Some(f) =>
        val file = f.toFile
        Option(file.getParentFile).foreach(_.mkdirs())
        file.getCanonicalPath
      case None => ""
    }
    SingleConnectionManager(SQLConfig(
      jdbcUrl = s"jdbc:duckdb:$path"
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