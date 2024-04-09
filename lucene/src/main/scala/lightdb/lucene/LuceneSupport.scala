package lightdb.lucene

import cats.effect.IO
import lightdb._
import lightdb.index.{IndexManager, IndexSupport, IndexedField, Indexer}
import lightdb.lucene.index._
import lightdb.query.{IndexContext, PagedResults, Query}
import org.apache.lucene.search.{MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField}
import org.apache.lucene.{document => ld}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}

trait LuceneSupport[D <: Document[D]] extends IndexSupport[D, LuceneIndexer[_]] {
  override protected lazy val indexer: LuceneIndexer[D] = LuceneIndexer[D](this)
  override lazy val index: LuceneIndexManager[D] = LuceneIndexManager(this)

  val _id: StringField[D] = index("_id").string(_._id.value, store = true)

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = indexer.withSearchContext(f)

  override protected def postSet(doc: D): IO[Unit] = for {
    fields <- IO(index.fields.flatMap { field =>
      field.createFields(doc)
    })
    _ = indexer.addDoc(doc._id, fields)
    _ <- super.postSet(doc)
  } yield ()

  override protected def postDelete(doc: D): IO[Unit] = indexer.delete(doc._id).flatMap { _ =>
    super.postDelete(doc)
  }
}

case class LuceneIndexContext[D <: Document[D]](page: PagedResults[D],
                                                context: SearchContext[D],
                                                lastScoreDoc: Option[ScoreDoc]) extends IndexContext[D] {
  override def nextPage(): IO[Option[PagedResults[D]]] = if (page.hasNext) {
    query.doSearch(
      context = context,
      offset = offset + query.pageSize,
      after = lastScoreDoc
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}

case class LuceneIndexManager[D <: Document[D]](collection: Collection[D]) extends IndexManager[D, LuceneIndexedField[_, D]] {
  def apply(name: String): IndexedFieldBuilder = IndexedFieldBuilder(name)

  case class IndexedFieldBuilder(fieldName: String) {
    def tokenized(f: D => String): TokenizedField[D] = TokenizedField(fieldName, collection, f)
    def string(f: D => String, store: Boolean = false): StringField[D] = StringField(fieldName, collection, f, store)
    def int(f: D => Int): IntField[D] = IntField(fieldName, collection, f)
    def long(f: D => Long): LongField[D] = LongField(fieldName, collection, f)
    def float(f: D => Float): FloatField[D] = FloatField(fieldName, collection, f)
    def double(f: D => Double): DoubleField[D] = DoubleField(fieldName, collection, f)
    def bigDecimal(f: D => BigDecimal): BigDecimalField[D] = BigDecimalField(fieldName, collection, f)
  }
}

/*
FILTER: def apply[D <: Document[D]](f: => LuceneQuery): Filter[D] = new Filter[D] {
    override protected[lightdb] def asQuery: LuceneQuery = f
  }
 */

trait LuceneIndexedField[F, D <: Document[D]] extends IndexedField[F, D] {
  protected[lightdb] def createFields(doc: D): List[ld.Field]
  protected[lightdb] def sortType: SortField.Type

  collection.asInstanceOf[LuceneSupport].index.register(this)
}

case class LuceneIndexer[D <: Document[D]](collection: Collection[D],
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

  override def commit(): IO[Unit] = IO(commitBlocking())

  override def count(): IO[Int] = withSearchContext { context =>
    IO(context.indexSearcher.count(new MatchAllDocsQuery))
  }
}
