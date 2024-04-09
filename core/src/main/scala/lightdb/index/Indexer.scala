package lightdb.index

import cats.effect.IO
import lightdb.{Collection, Document, Id}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}

case class Indexer[D <: Document[D]](collection: Collection[D],
                                     persistent: Boolean = true,
                                     analyzer: Analyzer = new StandardAnalyzer) {
  private lazy val path: Option[Path] = if (persistent) {
    val p = collection.db.directory.resolve(collection.collectionName).resolve("index")
    Files.createDirectories(p)
    Some(p)
  } else {
    None
  }
  private lazy val directory = path
    .map(p => FSDirectory.open(p))
    .getOrElse(new ByteBuffersDirectory())
  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val indexWriter = new IndexWriter(directory, config)

  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val parser = new QueryParser("_id", analyzer)

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = {
    val indexSearcher = searcherManager.acquire()
    val context = SearchContext(collection, indexSearcher)
    f(context)
      .guarantee(IO(searcherManager.release(indexSearcher)))
  }

  private[lightdb] def addDoc(id: Id[D], fields: List[LuceneField]): Unit = if (fields.length > 1) {
    val document = new LuceneDocument
    fields.foreach(document.add)
    indexWriter.updateDocument(new Term("_id", id.value), document)
  }

  private[lightdb] def delete(id: Id[D]): IO[Unit] = IO {
    indexWriter.deleteDocuments(parser.parse(s"_id:${id.value}"))
  }

  private def commitBlocking(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
    searcherManager.maybeRefreshBlocking()
  }

  def commit(): IO[Unit] = IO(commitBlocking())

  def count(): IO[Int] = withSearchContext { context =>
    IO(context.indexSearcher.count(new MatchAllDocsQuery))
  }
}
