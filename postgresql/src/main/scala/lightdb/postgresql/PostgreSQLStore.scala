package lightdb.postgresql

import fabric.define.DefType
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.sql.{SQLState, SQLStore}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.Task

import java.nio.file.Path
import java.sql.Connection

class PostgreSQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         path: Option[Path],
                                                                         model: Model,
                                                                         val connectionManager: ConnectionManager,
                                                                         val storeMode: StoreMode[Doc, Model],
                                                                         lightDB: LightDB,
                                                                         storeManager: StoreManager) extends SQLStore[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = PostgreSQLTransaction[Doc, Model]

  override def supportsSchemas: Boolean = true
  override def booleanAsNumber: Boolean = false

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    PostgreSQLTransaction(this, state, parent)
  }

  protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT * FROM information_schema.tables WHERE table_schema = ?;")
    try {
      ps.setString(1, lightDB.name)
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

  override protected def def2Type(name: String, d: DefType): String = d match {
    case DefType.Dec => "DOUBLE PRECISION"
    case DefType.Bool => "BOOLEAN"
    case _ => super.def2Type(name, d)
  }

  override protected def createUpsertSQL(): String = {
    val fieldNames = fields.map(_.name)
    val values = fields.map(field2Value)
    s"""MERGE INTO $fqn target
       |USING (VALUES (${values.mkString(", ")})) AS source (${fieldNames.mkString(", ")})
       |ON target._id = source._id
       |WHEN MATCHED THEN
       |    UPDATE SET ${fieldNames.map(f => s"$f = source.$f").mkString(", ")}
       |WHEN NOT MATCHED THEN
       |    INSERT (${fieldNames.mkString(", ")}) VALUES (${fieldNames.map(f => s"source.$f").mkString(", ")});
       |""".stripMargin
  }
}