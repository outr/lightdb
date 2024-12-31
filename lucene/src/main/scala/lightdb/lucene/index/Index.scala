package lightdb.lucene.index

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{BaseDirectory, ByteBuffersDirectory, FSDirectory}

import java.nio.file.{Files, Path}

case class Index(path: Option[Path]) {
  lazy val analyzer: Analyzer = new StandardAnalyzer
  lazy val parser = new QueryParser("_id", analyzer)

  private lazy val indexDirectory: BaseDirectory = path.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(1_000)
    c
  }
  lazy val indexWriter = new IndexWriter(indexDirectory, config)
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val taxonomyPath = path.map(p => p.resolve("taxonomy"))
  private var taxonomyLoaded = false
  private lazy val taxonomyDirectory: BaseDirectory = taxonomyPath.map { path =>
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    }
    taxonomyLoaded = true
    FSDirectory.open(path)
  }.getOrElse(new ByteBuffersDirectory)
  lazy val taxonomyWriter: DirectoryTaxonomyWriter = new DirectoryTaxonomyWriter(taxonomyDirectory)

  def createIndexSearcher(): IndexSearcher = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.acquire()
  }

  def createTaxonomyReader(): TaxonomyReader = new DirectoryTaxonomyReader(taxonomyWriter)

  def releaseIndexSearch(indexSearcher: IndexSearcher): Unit = searcherManager.release(indexSearcher)

  def releaseTaxonomyReader(taxonomyReader: TaxonomyReader): Unit = taxonomyReader.close()

  def commit(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
    if (taxonomyLoaded) {
      taxonomyWriter.commit()
    }
  }

  def rollback(): Unit = {
    indexWriter.rollback()
    if (taxonomyLoaded) {
      taxonomyWriter.rollback()
    }
  }

  def dispose(): Unit = {
    commit()
    indexWriter.close()
    if (taxonomyLoaded) {
      taxonomyDirectory.close()
    }
  }
}