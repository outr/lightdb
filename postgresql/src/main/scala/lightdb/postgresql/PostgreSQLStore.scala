package lightdb.postgresql

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLStore
import lightdb.sql.connect.ConnectionManager
import lightdb.store.StoreMode

import java.sql.Connection

class PostgreSQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         model: Model,
                                                                         val connectionManager: ConnectionManager,
                                                                         val storeMode: StoreMode[Doc, Model]) extends SQLStore[Doc, Model](name, model) {
  protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT * FROM information_schema.tables;")
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

  override protected def createUpsertSQL(): String = {
    val fieldNames = fields.map(_.name)
    val values = fields.map(field2Value)
    s"""MERGE INTO $name target
       |USING (VALUES (${values.mkString(", ")})) AS source (${fieldNames.mkString(", ")})
       |ON target._id = source._id
       |WHEN MATCHED THEN
       |    UPDATE SET ${fieldNames.map(f => s"$f = source.$f").mkString(", ")}
       |WHEN NOT MATCHED THEN
       |    INSERT (${fieldNames.mkString(", ")}) VALUES (${fieldNames.map(f => s"source.$f").mkString(", ")});
       |""".stripMargin
  }
}