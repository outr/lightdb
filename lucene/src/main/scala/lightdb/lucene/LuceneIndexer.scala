package lightdb.lucene

import cats.effect.IO
import lightdb.index.{IndexSupport, Indexer}
import lightdb.lucene.index.{BigDecimalField, DoubleField, FloatField, IntField, LongField, StringField, TokenizedField}
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

case class LuceneIndexer[D <: Document[D]](indexSupport: IndexSupport[D],
                                           persistent: Boolean = true,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D] {
  private lazy val path: Option[Path] = if (persistent) {
    val p = indexSupport.db.directory.resolve(indexSupport.collectionName).resolve("index")
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

  def apply(name: String): IndexedFieldBuilder = IndexedFieldBuilder(name)

  override def commit(): IO[Unit] = IO(commitBlocking())

  override def count(): IO[Int] = withSearchContext { context =>
    IO(context.indexSupport.asInstanceOf[LuceneSupport[D]].indexSearcher(context).count(new MatchAllDocsQuery))
  }

  case class IndexedFieldBuilder(fieldName: String) {
    def tokenized(f: D => String): TokenizedField[D] = TokenizedField(fieldName, indexSupport, f)
    def string(f: D => String, store: Boolean = false): StringField[D] = StringField(fieldName, indexSupport, f, store)
    def int(f: D => Int): IntField[D] = IntField(fieldName, indexSupport, f)
    def long(f: D => Long): LongField[D] = LongField(fieldName, indexSupport, f)
    def float(f: D => Float): FloatField[D] = FloatField(fieldName, indexSupport, f)
    def double(f: D => Double): DoubleField[D] = DoubleField(fieldName, indexSupport, f)
    def bigDecimal(f: D => BigDecimal): BigDecimalField[D] = BigDecimalField(fieldName, indexSupport, f)
  }
}