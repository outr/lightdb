package lightdb.duckdb

import fabric.define.DefType
import lightdb.Document
import lightdb.sql.SQLSupport

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager}

trait DuckDBSupport[D <: Document[D]] extends SQLSupport[D] {
  private lazy val path: Path = {
    val p = collection.db.directory.resolve(collection.collectionName).resolve("duckdb.db")
    Files.createDirectories(p.getParent)
    p
  }
  // TODO: Should each collection have a connection?

  override protected def enableAutoCommit: Boolean = true

  override protected def createTable(): String = {
    val indexes = index.fields.map { i =>
      if (i.fieldName == "_id") {
        "_id VARCHAR PRIMARY KEY"
      } else {
        val t = i.rw.definition match {
          case DefType.Str => "VARCHAR"
          case DefType.Int => "INTEGER"
          case d => throw new UnsupportedOperationException(s"${i.fieldName} has an unsupported type: $d")
        }
        s"${i.fieldName} $t"
      }
    }.mkString(", ")
    val sql = s"CREATE TABLE IF NOT EXISTS ${collection.collectionName}($indexes)"
    scribe.info(sql)
    sql
  }

  override protected def createConnection(): Connection = {
    Class.forName("org.duckdb.DuckDBDriver")
    val url = s"jdbc:duckdb:${path.toFile.getCanonicalPath}"
    DriverManager.getConnection(url)
  }

  override protected def truncateSQL: String = s"TRUNCATE ${collection.collectionName}"
}
