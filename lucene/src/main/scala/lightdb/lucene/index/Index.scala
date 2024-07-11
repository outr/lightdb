package lightdb.lucene.index

import lightdb.lucene.LuceneTransaction
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, memory}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}

import java.nio.file.Path

case class Index(path: Option[Path]) {
  lazy val analyzer: Analyzer = new StandardAnalyzer
  lazy val parser = new QueryParser("_id", analyzer)

  private lazy val directory = path.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(1_000)
    c
  }
  lazy val indexWriter = new IndexWriter(directory, config)
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  def createIndexSearcher(): IndexSearcher = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.acquire()
  }

  def releaseIndexSearch(indexSearcher: IndexSearcher): Unit = searcherManager.release(indexSearcher)

  def commit(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
  }

  def rollback(): Unit = {
    indexWriter.rollback()
  }
}