package lightdb.h2

import lightdb.document.{Document, DocumentModel}
import lightdb.sql.{ConnectionManager, HikariConnectionManager, SQLConfig, SQLIndexer, SingleConnectionManager}

import java.nio.file.{Files, Path}

case class H2Indexer[D <: Document[D], M <: DocumentModel[D]]() extends SQLIndexer[D, M] {
  private lazy val path: Path = {
    val p = collection.db.directory.get.resolve(collection.name).resolve("h2")
    Files.createDirectories(p.getParent)
    p
  }
  override protected lazy val config: SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:h2:${path.toFile.getCanonicalPath}"
  )
  override protected lazy val connectionManager: ConnectionManager[D] = HikariConnectionManager(config)

  override protected def upsertPrefix: String = "MERGE"

  override protected def concatPrefix: String = "LISTAGG"
}
