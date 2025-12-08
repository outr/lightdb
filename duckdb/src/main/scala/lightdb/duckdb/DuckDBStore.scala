package lightdb.duckdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.sql.{SQLDatabase, SQLState, SQLStore}
import lightdb.store.{CollectionManager, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
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
    val cols = fields.map(_.name)
    val values = cols.map(_ => "?")
    val assignments = fields.filterNot(_ == model._id).map(f => s"${f.name}=excluded.${f.name}")
    val update = if (assignments.nonEmpty) assignments.mkString(", ") else s"${model._id.name}=${model._id.name}"
    s"INSERT INTO $fqn(${cols.mkString(", ")}) VALUES(${values.mkString(", ")}) ON CONFLICT(${model._id.name}) DO UPDATE SET $update"
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[DuckDBTransaction[Doc, Model]] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    DuckDBTransaction(this, state, parent)
  }

  // TODO: Use DuckDB's Appender for better performance
  /*override def insert(doc: Doc)(transaction: Transaction[Doc]): Unit = {
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