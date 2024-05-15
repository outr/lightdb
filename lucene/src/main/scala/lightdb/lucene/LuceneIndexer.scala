package lightdb.lucene

import cats.effect.IO
import lightdb.index.{IndexSupport, Indexer}
import lightdb.query.SearchContext
import lightdb.{Document, Id}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import fabric.rw._
import lightdb.model.AbstractCollection

case class LuceneIndexer[D <: Document[D]](indexSupport: IndexSupport[D],
                                           collection: AbstractCollection[D],
                                           persistent: Boolean = true,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D] {
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

  private[lucene] val contextMapping = new ConcurrentHashMap[SearchContext[D], IndexSearcher]

  override def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = {
    val indexSearcher = searcherManager.acquire()
    val context = SearchContext(indexSupport)
    contextMapping.put(context, indexSearcher)
    f(context)
      .guarantee(IO {
        contextMapping.remove(context)
        searcherManager.release(indexSearcher)
      })
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

  def apply[F](name: String,
               get: D => List[F],
               store: Boolean = false,
               tokenized: Boolean = false)
              (implicit rw: RW[F]): LuceneIndex[F, D] = LuceneIndex(
    fieldName = name,
    indexSupport = indexSupport,
    get = get,
    store = store,
    tokenized = tokenized
  )

  def one[F](name: String,
             get: D => F,
             store: Boolean = false,
             tokenized: Boolean = false)
            (implicit rw: RW[F]): LuceneIndex[F, D] = apply[F](name, doc => List(get(doc)), store, tokenized)

  override def commit(): IO[Unit] = IO(commitBlocking())

  override def count(): IO[Int] = withSearchContext { context =>
    IO(context.indexSupport.asInstanceOf[LuceneSupport[D]].indexSearcher(context).count(new MatchAllDocsQuery))
  }
}