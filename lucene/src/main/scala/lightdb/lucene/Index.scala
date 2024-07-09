package lightdb.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, memory}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{SearcherFactory, SearcherManager}
import org.apache.lucene.store.FSDirectory

import java.nio.file.Path

trait Index {
  protected lazy val analyzer: Analyzer = new StandardAnalyzer
  protected lazy val parser = new QueryParser("_id", analyzer)
}

class MemoryIndex extends Index {
  private lazy val lucene = new memory.MemoryIndex
}

case class FSIndex(path: Path) extends Index {
  private lazy val directory = FSDirectory.open(path)
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(1_000)
    c
  }
  private lazy val indexWriter = new IndexWriter(directory, config)
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)
}