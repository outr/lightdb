package lightdb.duckdb

import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexer, IndexerManager}
import lightdb.sql.{ConnectionManager, SQLConfig, SQLIndexer, SingleConnectionManager}
import lightdb.transaction.{Transaction, TransactionKey}
import org.duckdb.{DuckDBAppender, DuckDBConnection}

import java.nio.file.{Files, Path}

case class DuckDBIndexer[D <: Document[D], M <: DocumentModel[D]]() extends SQLIndexer[D, M] {
  private lazy val appenderKey: TransactionKey[DuckDBAppender] = TransactionKey("duckDBAppender")

  private lazy val path: Path = {
    val p = collection.db.directory.get.resolve(collection.name).resolve("duckdb.db")
    Files.createDirectories(p.getParent)
    p
  }
  override protected lazy val config: SQLConfig = SQLConfig(
    jdbcUrl = s"jdbc:duckdb:${path.toFile.getCanonicalPath}",
    autoCommit = true
  )
  override protected lazy val connectionManager: ConnectionManager[D] = SingleConnectionManager(config)

  /*override protected def indexDoc(doc: D)(implicit transaction: Transaction[D]): IO[Unit] = if (doc._id.persisted) {
    super.indexDoc(doc)
  } else {
    IO.blocking {
      val connection = getConnection.asInstanceOf[DuckDBConnection]
      val appender = transaction
        .getOrCreate(appenderKey, connection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, collection.name))
      appender.beginRow()
      indexes.foreach { index =>
        val json = index.getJson(doc).headOption.getOrElse(Null)
        json match {
          case Null => appender.append(null)
          case Str(s, _) => appender.append(s)
          case Bool(b, _) => appender.append(b)
          case NumInt(l, _) => appender.append(l)
          case NumDec(bd, _) => appender.appendBigDecimal(bd.bigDecimal)
          case _ => appender.append(JsonFormatter.Compact(json))
        }
      }
      appender.endRow()
    }
  }

  override def commit(transaction: Transaction[D]): IO[Unit] = super.commit(transaction).flatMap { _ =>
    IO.blocking {
      transaction.get(appenderKey).foreach { appender =>
        appender.close()
      }
    }
  }*/
}

object DuckDBIndexer extends IndexerManager {
  override def create[D <: Document[D], M <: DocumentModel[D]](): Indexer[D, M] = DuckDBIndexer()
}