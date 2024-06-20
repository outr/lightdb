package lightdb.lucene

import cats.effect.IO
import fabric.define.DefType
import lightdb.index.{Index, Indexer, Materialized}
import lightdb.Id
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, SortedNumericSortField, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{LatLonDocValuesField, Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}
import fabric.rw._
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.query.{Query, SearchResults, Sort, SortDirection}
import lightdb.transaction.{Transaction, TransactionKey}

case class LuceneIndexer[D <: Document[D], M <: DocumentModel[D]](model: M,
                                           batchSize: Int = 512,
                                           persistent: Boolean = true,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D, M] {
  private var collection: Collection[D, _] = _
  model.listener += this

  private lazy val indexSearcherKey: TransactionKey[IndexSearcher] = TransactionKey("indexSearcher")

  override def init(collection: Collection[D, _]): IO[Unit] = super.init(collection).map { _ =>
    this.collection = collection
  }

  override def transactionEnd(transaction: Transaction[D]): IO[Unit] = super.transactionEnd(transaction).map { _ =>
    transaction.get(indexSearcherKey).foreach { indexSearcher =>
      searcherManager.release(indexSearcher)
    }
  }

  private lazy val path: Option[Path] = if (persistent) {
    val p = collection.db.directory.resolve(collection.name).resolve("index")
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

  private[lightdb] def addDoc(id: Id[D], fields: List[LuceneField]): Unit = if (fields.length > 1) {
    val document = new LuceneDocument
    fields.foreach(document.add)
    indexWriter.updateDocument(new Term("_id", id.value), document)
  }

  private[lightdb] def delete(id: Id[D]): IO[Unit] = IO.blocking {
    indexWriter.deleteDocuments(parser.parse(s"_id:${id.value}"))
  }

  def truncate(): IO[Unit] = IO.blocking {
    indexWriter.deleteAll()
  }

  private def commitBlocking(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
    searcherManager.maybeRefreshBlocking()
  }

  override def apply[F](name: String,
               get: D => List[F],
               store: Boolean = false,
               sorted: Boolean = false,
               tokenized: Boolean = false)
              (implicit rw: RW[F]): Index[F, D] = LuceneIndex(
    name = name,
    indexer = this,
    get = get,
    store = store,
    sorted = sorted,
    tokenized = tokenized
  )

  override def doSearch[V](query: Query[D, M],
                           transaction: Transaction[D],
                           conversion: Conversion[V]): IO[SearchResults[D, V]] = IO.blocking {
    val q: LuceneQuery = query.filter.map(_.asInstanceOf[LuceneFilter[D]].asQuery()).getOrElse(new MatchAllDocsQuery)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = transaction.getOrCreate(indexSearcherKey, searcherManager.acquire())
    val topFieldDocs: TopFieldDocs = indexSearcher.search(q, batchSize, s, query.scoreDocs)
    val scoreDocs: List[ScoreDoc] = topFieldDocs
      .scoreDocs
      .toList
      .slice(query.offset, query.offset + query.limit.getOrElse(Int.MaxValue - query.offset))
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val idsAndScores = scoreDocs.map(doc => Id[D](storedFields.document(doc.doc).get("_id")) -> doc.score.toDouble)
    // TODO: Support streaming through entire resultset
//    val last = scoreDocs.lastOption
    val idStream = fs2.Stream(idsAndScores: _*)
    val stream: fs2.Stream[IO, (V, Double)] = conversion match {
      case Conversion.Id => idStream
      case Conversion.Doc => idStream.evalMap {
        case (id, score) => collection(id)(transaction).map(doc => doc -> score)
      }
      case Conversion.Materialized(indexes) => ???   // TODO: Re-evaluate
      case _ => throw new UnsupportedOperationException(s"Invalid Conversion: $conversion")
    }

    SearchResults(
      offset = query.offset,
      limit = query.limit,
      total = Some(total),    // TODO: Is it faster if I turn this off?
      scoredStream = stream,
      transaction = transaction
    )
  }

  override def aggregate(query: AggregateQuery[D, M])
                        (implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] =
    throw new UnsupportedOperationException("Aggregate functions not supported in Lucene currently")

  //  override def size: IO[Int] = withSearchContext { context =>
//    IO.blocking(context.indexSupport.asInstanceOf[LuceneSupport[D]].indexSearcher(context).count(new MatchAllDocsQuery))
//  }

  private def sort2SortField(sort: Sort): SortField = sort match {
    case Sort.BestMatch => SortField.FIELD_SCORE
    case Sort.IndexOrder => SortField.FIELD_DOC
    case Sort.ByField(field, dir) =>
      val f = field.asInstanceOf[LuceneIndex[_, D]]
      f.rw.definition match {
        case DefType.Int => new SortedNumericSortField(field.name, f.sortType, dir == SortDirection.Descending)
        case DefType.Str => new SortField(field.name, f.sortType, dir == SortDirection.Descending)
        case d => throw new RuntimeException(s"Unsupported sort definition: $d")
      }
    case Sort.ByDistance(field, from) =>
      val f = field.asInstanceOf[LuceneIndex[_, D]]
      LatLonDocValuesField.newDistanceSort(f.fieldSortName, from.latitude, from.longitude)
  }
}