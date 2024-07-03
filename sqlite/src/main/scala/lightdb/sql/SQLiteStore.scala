package lightdb.sql

import lightdb.sql.connect.ConnectionManager
import lightdb.{LightDB, Transaction}
import lightdb.doc.DocModel
import lightdb.store.{Store, StoreManager}
import org.sqlite.SQLiteConfig

import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Connection

class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Option[Path]) extends SQLStore[Doc, Model] {
  private lazy val connection: Connection = {
    val path = file match {
      case Some(f) =>
        val file = f.toFile
        Option(file.getParentFile).foreach(_.mkdirs())
        file.getCanonicalPath
      case None => ":memory:"
    }

    val config = new SQLiteConfig
    config.enableLoadExtension(true)
    val c = config.createConnection(s"jdbc:sqlite:$path")
    c.setAutoCommit(false)
    c
  }

  override protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    super.initTransaction()

    val file = Files.createTempFile("mod_spatialite", ".so")
    val input = getClass.getClassLoader.getResourceAsStream("mod_spatialite.so")
    Files.copy(input, file, StandardCopyOption.REPLACE_EXISTING)
    executeUpdate(s"SELECT load_extension('${file.toAbsolutePath.toString}');")
  }

  override protected object connectionManager extends ConnectionManager[Doc] {
    override def getConnection(implicit transaction: Transaction[Doc]): Connection = connection

    override def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

    override def releaseConnection(implicit transaction: Transaction[Doc]): Unit = {}
  }
}

object SQLiteStore extends StoreManager {
  override def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String): Store[Doc, Model] =
    new SQLiteStore[Doc, Model](db.directory.map(_.resolve(name)))
}