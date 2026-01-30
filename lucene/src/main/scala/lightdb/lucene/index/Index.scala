package lightdb.lucene.index

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig, TieredMergePolicy}
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{BaseDirectory, ByteBuffersDirectory, FSDirectory}
import profig.Profig
import fabric.rw.*

import java.nio.file.{Files, Path}

case class Index(path: Option[Path]) {
  lazy val analyzer: Analyzer = new StandardAnalyzer

  private lazy val indexDirectory: BaseDirectory = path.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(Profig("lightdb.lucene.ramBufferMB").opt[Double].getOrElse(2_000d))
    c.setMaxBufferedDocs(Profig("lightdb.lucene.maxBufferedDocs").opt[Int].getOrElse(10_000))
    c.setMergePolicy(new TieredMergePolicy)
    c.setMergeScheduler(new ConcurrentMergeScheduler)
    c.setUseCompoundFile(Profig("lightdb.lucene.useCompoundFile").opt[Boolean].getOrElse(false))
    c
  }
  lazy val indexWriter = new IndexWriter(indexDirectory, config)
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val taxonomyPath = path.map(p => p.resolve("taxonomy"))
  private var taxonomyLoaded = false
  private lazy val taxonomyDirectory: BaseDirectory = taxonomyPath.map { path =>
    if !Files.exists(path) then {
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
    if taxonomyLoaded then {
      taxonomyWriter.commit()
    }
  }

  def rollback(): Unit = {
    indexWriter.rollback()
    if taxonomyLoaded then {
      taxonomyWriter.rollback()
    }
  }

  def dispose(): Unit = {
    commit()
    indexWriter.close()
    if taxonomyLoaded then {
      taxonomyDirectory.close()
    }
  }
}