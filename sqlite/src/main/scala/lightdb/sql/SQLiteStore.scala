package lightdb.sql

import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.Converter
import lightdb.doc.DocModel

import java.nio.file.Path
import java.sql.ResultSet

case class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Path, converter: Converter[ResultSet, Doc]) extends SQLStore[Doc, Model] {
  override protected lazy val config: SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:sqlite:${file.toFile.getCanonicalPath}"
  )
  override protected lazy val connectionManager: ConnectionManager[Doc] = SingleConnectionManager(config)
}
