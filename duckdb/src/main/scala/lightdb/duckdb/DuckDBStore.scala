package lightdb.duckdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLStore}
import lightdb.store.{Store, StoreManager, StoreMode}

import java.nio.file.Path
import java.sql.Connection

class DuckDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     model: Model,
                                                                     val connectionManager: ConnectionManager,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     storeManager: StoreManager) extends SQLStore[Doc, Model](name, model, storeManager) {
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

object DuckDBStore extends StoreManager {
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

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String, model: Model, file: Option[Path], storeMode: StoreMode[Doc, Model]): DuckDBStore[Doc, Model] = {
    new DuckDBStore[Doc, Model](
      name = name,
      model = model,
      connectionManager = singleConnectionManager(file),
      storeMode = storeMode,
      storeManager = this
    )
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) => new DuckDBStore[Doc, Model](
        name = name,
        model = model,
        connectionManager = sqlDB.connectionManager,
        storeMode = storeMode,
        storeManager = this
      )
      case None => apply[Doc, Model](name, model, db.directory.map(_.resolve(s"$name.duckdb")), storeMode)
    }
  }
}