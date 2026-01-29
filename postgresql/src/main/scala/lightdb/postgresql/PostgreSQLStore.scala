package lightdb.postgresql

import fabric.define.DefType
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.sql.connect.ConnectionManager
import lightdb.sql.{SQLState, SQLStore}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
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

  override protected def initConnection(connection: Connection): Unit = {
    super.initConnection(connection)

    val s = connection.createStatement()
    try {
      // Create pg_trgm to support extremely large columns with efficient filtering
      s.executeUpdate("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    } finally {
      s.close()
    }
  }

  override protected def createIndexSQL(index: Field.Indexed[Doc, _]): String = {
    index match {
      case _: Field.Tokenized[Doc @unchecked] =>
        // Use trigram GIN index for tokenized fields to accelerate ILIKE/LIKE searches
        s"CREATE INDEX IF NOT EXISTS ${name}_${index.name}_trgm_idx ON $fqn USING gin (${index.name} gin_trgm_ops)"
      case _ if index.isArr =>
        // Use pg_trgm gin for arrays
        s"CREATE INDEX IF NOT EXISTS ${name}_${index.name}_idx ON $fqn USING gin (${index.name} gin_trgm_ops)"
      case _ => super.createIndexSQL(index)
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = Task {
    val state = SQLState(connectionManager, this, Store.CacheQueries)
    PostgreSQLTransaction(this, state, parent, writeHandlerFactory)
  }

  protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement("SELECT * FROM information_schema.tables WHERE table_schema = ?;")
    try {
      ps.setString(1, lightDB.name)
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while rs.next() do {
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

  override protected def field2Value(field: Field[Doc, _]): String = {
    def lookup(definition: DefType): String = definition match {
      case DefType.Opt(dt) => lookup(dt)
      case DefType.Dec => "?::double precision"
      case DefType.Int => "?::bigint"
      case DefType.Bool => "?::boolean"
      case _ => "?"
    }
    lookup(field.getRW().definition)
  }
}