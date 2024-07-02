package lightdb.sql

import lightdb.sql.connect.{ConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.LightDB
import lightdb.doc.DocModel
import lightdb.store.{Store, StoreManager}

import java.nio.file.Path

class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Option[Path]) extends SQLStore[Doc, Model] {
  override protected lazy val config: SQLConfig = {
    val path = file match {
      case Some(f) =>
        val file = f.toFile
        Option(file.getParentFile).foreach(_.mkdirs())
        file.getCanonicalPath
      case None => ":memory:"
    }
    SQLConfig(
      jdbcUrl = s"jdbc:sqlite:$path"
    )
  }
  override protected lazy val connectionManager: ConnectionManager[Doc] = SingleConnectionManager(config)
}

object SQLiteStore extends StoreManager {
  override def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String): Store[Doc, Model] =
    new SQLiteStore[Doc, Model](db.directory.map(_.resolve(name)))
}