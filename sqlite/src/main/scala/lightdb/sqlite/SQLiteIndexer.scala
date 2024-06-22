package lightdb.sqlite

import lightdb.document.{Document, DocumentModel}
import lightdb.sql.SQLIndexer

import java.nio.file.{Files, Path}

case class SQLiteIndexer[D <: Document[D], M <: DocumentModel[D]]() extends SQLIndexer[D, M] {
  private lazy val path: Path = {
    val p = collection.db.directory.resolve(collection.name).resolve("sqlite.db")
    Files.createDirectories(p.getParent)
    p
  }

  override protected lazy val jdbcUrl: String = s"jdbc:sqlite:${path.toFile.getCanonicalPath}"
}
