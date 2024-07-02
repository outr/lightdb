package lightdb.sql

import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.LightDB
import lightdb.doc.DocModel
import lightdb.store.{Store, StoreManager}

import java.nio.file.Path

case class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Path) extends SQLStore[Doc, Model] {
  override protected lazy val config: SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:sqlite:${file.toFile.getCanonicalPath}"
  )
  override protected lazy val connectionManager: ConnectionManager[Doc] = SingleConnectionManager(config)
}

object SQLiteStore extends StoreManager {
  override def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String): Store[Doc, Model] = ???
}