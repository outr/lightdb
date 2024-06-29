package lightdb.sqlite

import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexer, IndexerManager}
import lightdb.sql.{ConnectionManager, HikariConnectionManager, SQLConfig, SQLIndexer, SingleConnectionManager}

import java.nio.file.{Files, Path}

case class SQLiteIndexer[D <: Document[D], M <: DocumentModel[D]]() extends SQLIndexer[D, M] {
  private lazy val path: Path = {
    val p = collection.db.directory.get.resolve(collection.name).resolve("sqlite.db")
    Files.createDirectories(p.getParent)
    p
  }
  override protected lazy val config: SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:sqlite:${path.toFile.getCanonicalPath}"
  )
  override protected lazy val connectionManager: ConnectionManager[D] = SingleConnectionManager(config)
}

object SQLiteIndexer extends IndexerManager {
  override def create[D <: Document[D], M <: DocumentModel[D]](): Indexer[D, M] = SQLiteIndexer()
}