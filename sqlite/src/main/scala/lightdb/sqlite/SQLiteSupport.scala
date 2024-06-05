package lightdb.sqlite

import lightdb.Document
import lightdb.sql.SQLSupport

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager}

trait SQLiteSupport[D <: Document[D]] extends SQLSupport[D] {
  private lazy val path: Path = {
    val p = collection.db.directory.resolve(collection.collectionName).resolve("sqlite.db")
    Files.createDirectories(p.getParent)
    p
  }
  // TODO: Should each collection have a connection?

  override protected def createConnection(): Connection = {
    val url = s"jdbc:sqlite:${path.toFile.getCanonicalPath}"
    DriverManager.getConnection(url)
  }
}
