package lightdb.duckdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLStore
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.duckdb.DuckDBConnection

import java.nio.file.Path
import java.sql.Connection

class DuckDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](file: Option[Path], val storeMode: StoreMode) extends SQLStore[Doc, Model] {
  override protected lazy val connectionManager: ConnectionManager = {
    val path = file match {
      case Some(f) =>
        val file = f.toFile
        Option(file.getParentFile).foreach(_.mkdirs())
        file.getCanonicalPath
      case None => ""
    }
    SingleConnectionManager(SQLConfig(
      jdbcUrl = s"jdbc:duckdb:$path",
      autoCommit = true     // TODO: Figure out how to make DuckDB with autoCommit = false
    ))
  }

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
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB, name: String, storeMode: StoreMode): Store[Doc, Model] =
    new DuckDBStore(db.directory.map(_.resolve(s"$name.duckdb.db")), storeMode)
}