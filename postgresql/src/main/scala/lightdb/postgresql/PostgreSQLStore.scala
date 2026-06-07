package lightdb.postgresql

import fabric.define.{DefType, Definition}
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.sql.connect.ConnectionManager
import lightdb.sql.{SQLState, SQLStore, SqlIdent}
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
      // pgvector is required only when the model declares a vector field; enabling it lets the
      // `vector(N)` columns and HNSW indexes below be created.
      if (fields.exists(_.isVector)) {
        s.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector")
      }
    } finally {
      s.close()
    }
  }

  override protected def createIndexSQL(index: Field.Indexed[Doc, _]): String = {
    val col = SqlIdent.quote(index.name)
    index match {
      case vi: Field.VectorIndex[Doc @unchecked] =>
        // HNSW approximate-nearest-neighbor index, with the operator class matching the field's metric.
        val ops = vi.metric match {
          case lightdb.vector.VectorMetric.Cosine => "vector_cosine_ops"
          case lightdb.vector.VectorMetric.Euclidean => "vector_l2_ops"
          case lightdb.vector.VectorMetric.DotProduct => "vector_ip_ops"
        }
        s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${index.name}_idx")} ON $fqn USING hnsw ($col $ops)"
      case _: Field.Tokenized[Doc @unchecked] =>
        // Use trigram GIN index for tokenized fields to accelerate ILIKE/LIKE searches
        s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${index.name}_trgm_idx")} ON $fqn USING gin ($col gin_trgm_ops)"
      case _ if index.isArr =>
        // Use pg_trgm gin for arrays
        s"CREATE INDEX IF NOT EXISTS ${SqlIdent.quote(s"${name}_${index.name}_idx")} ON $fqn USING gin ($col gin_trgm_ops)"
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

  override protected def columnType(field: Field[Doc, _]): String =
    if field.isVector then s"vector(${field.vectorDimension.get})" else super.columnType(field)

  override protected def def2Type(name: String, d: Definition): String = d.defType match {
    case DefType.Dec => "DOUBLE PRECISION"
    case DefType.Bool => "BOOLEAN"
    case _ => super.def2Type(name, d)
  }

  // PostgreSQL refuses an implicit `ALTER COLUMN ... TYPE` between incompatible
  // types (e.g. varchar -> bigint); the `USING <col>::<type>` cast is required.
  override protected def alterColumnTypeSQL(fieldName: String, newType: String): String = {
    val q = SqlIdent.quote(fieldName)
    s"ALTER TABLE $fqn ALTER COLUMN $q TYPE $newType USING $q::$newType"
  }

  override protected def createUpsertSQL(): String = {
    val fieldNames = fields.map(f => SqlIdent.quote(f.name))
    val values = fields.map(field2Value)
    val target = SqlIdent.quote("target")
    val source = SqlIdent.quote("source")
    val idCol = SqlIdent.quote("_id")
    s"""MERGE INTO $fqn $target
       |USING (VALUES (${values.mkString(", ")})) AS $source (${fieldNames.mkString(", ")})
       |ON $target.$idCol = $source.$idCol
       |WHEN MATCHED THEN
       |    UPDATE SET ${fieldNames.map(f => s"$f = $source.$f").mkString(", ")}
       |WHEN NOT MATCHED THEN
       |    INSERT (${fieldNames.mkString(", ")}) VALUES (${fieldNames.map(f => s"$source.$f").mkString(", ")});
       |""".stripMargin
  }

  override protected def field2Value(field: Field[Doc, _]): String = {
    if (field.isVector) return "?::vector"
    def lookup(definition: Definition): String = definition.defType match {
      case DefType.Opt(inner) => lookup(inner)
      case DefType.Dec => "?::double precision"
      case DefType.Int => "?::bigint"
      case DefType.Bool => "?::boolean"
      case _ => "?"
    }
    lookup(field.getRW().definition)
  }
}