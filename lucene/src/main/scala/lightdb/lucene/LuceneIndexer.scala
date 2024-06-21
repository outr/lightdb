package lightdb.lucene

import cats.effect.IO
import fabric._
import fabric.define.DefType
import lightdb.index.{Index, Indexed, Indexer, Materialized}
import lightdb.Id
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, SortedNumericSortField, TermQuery, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{DoubleField, DoublePoint, Field, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LongField, LongPoint, StringField, TextField, Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.filter.{CombinedFilter, EqualsFilter, Filter, InFilter, RangeDoubleFilter, RangeLongFilter}
import lightdb.query.{Query, SearchResults, Sort, SortDirection}
import lightdb.spatial.GeoPoint
import lightdb.transaction.{Transaction, TransactionKey}

case class LuceneIndexer[D <: Document[D], M <: DocumentModel[D]](model: M,
                                           batchSize: Int = 512,
                                           persistent: Boolean = true,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D, M] {
  private lazy val indexSearcherKey: TransactionKey[IndexSearcher] = TransactionKey("indexSearcher")

  override def transactionEnd(transaction: Transaction[D]): IO[Unit] = super.transactionEnd(transaction).map { _ =>
    transaction.get(indexSearcherKey).foreach { indexSearcher =>
      searcherManager.release(indexSearcher)
    }
  }

  override def postSet(doc: D, transaction: Transaction[D]): IO[Unit] = super.postSet(doc, transaction).flatMap { _ =>
    indexDoc(doc, model.asInstanceOf[Indexed[D]].indexes)
  }

  private def indexDoc(doc: D, indexes: List[Index[_, D]]): IO[Unit] = for {
    fields <- IO.blocking(indexes.flatMap { index =>
      createFields(index, doc)
    })
    _ = addDoc(doc._id, fields)
  } yield ()

  protected[lightdb] def createFields(index: Index[_, D], doc: D): List[LuceneField] = if (index.tokenized) {
    index.getJson(doc).flatMap {
      case Null => Nil
      case Str(s, _) => List(s)
      case f => throw new RuntimeException(s"Unsupported tokenized value: $f (${index.rw.definition})")
    }.map { value =>
      new LuceneField(index.name, value, if (index.store) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED)
    }
  } else {
    def fs: LuceneField.Store = if (index.store) Field.Store.YES else Field.Store.NO

    val filterField = index.getJson(doc).flatMap {
      case Null => None
      case Str(s, _) => Some(new StringField(index.name, s, fs))
      case Bool(b, _) => Some(new IntField(index.name, if (b) 1 else 0, fs))
      case NumInt(l, _) => Some(new LongField(index.name, l, fs))
      case NumDec(bd, _) => Some(new DoubleField(index.name, bd.toDouble, fs))
      case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
        case GeoPoint(latitude, longitude) => Some(new LatLonPoint(index.name, latitude, longitude))
        case ref => throw new RuntimeException(s"Unsupported object reference: $ref for JSON: $obj")
      }
      case json => throw new RuntimeException(s"Unsupported JSON: $json (${index.rw.definition})")
    }
    val sortField = if (index.sorted) {
      val separate = index.rw.definition.className.collect {
        case "lightdb.spatial.GeoPoint" => true
      }.getOrElse(false)
      val fieldSortName = if (separate) s"${index.name}Sort" else index.name
      index.getJson(doc).flatMap {
        case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
          case GeoPoint(latitude, longitude) => Some(new LatLonDocValuesField(fieldSortName, latitude, longitude))
          case _ => None
        }
        case _ => None
      }
    } else {
      Nil
    }
    filterField ::: sortField
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

  private def filter2Lucene(filter: Option[Filter[D]]): LuceneQuery = filter match {
    case Some(f) => f match {
      case f: EqualsFilter[_, D] => exactQuery(f.index, f.getJson)
      case f: InFilter[_, D] =>
        val queries = f.getJson.map(json => exactQuery(f.index, json))
        val b = new BooleanQuery.Builder
        b.setMinimumNumberShouldMatch(1)
        queries.foreach { q =>
          b.add(q, BooleanClause.Occur.SHOULD)
        }
        b.build()
      case CombinedFilter(filters) =>
        val queries = filters.map(f => filter2Lucene(Some(f)))
        val b = new BooleanQuery.Builder
        b.setMinimumNumberShouldMatch(1)
        queries.foreach { q =>
          b.add(q, BooleanClause.Occur.SHOULD)
        }
        b.build()
      case RangeLongFilter(index, from, to) => LongField.newRangeQuery(index.name, from, to)
      case RangeDoubleFilter(index, from, to) => DoubleField.newRangeQuery(index.name, from, to)
    }
    case None => new MatchAllDocsQuery
  }

  private def exactQuery(index: Index[_, D], json: Json): LuceneQuery = json match {
    case Str(s, _) if index.tokenized =>
      val b = new BooleanQuery.Builder
      s.split("\\s+").foreach(s => b.add(new TermQuery(new Term(index.name, s)), BooleanClause.Occur.MUST))
      b.build()
    case Str(s, _) => new TermQuery(new Term(index.name, s))
    case Bool(b, _) => IntPoint.newExactQuery(index.name, if (b) 1 else 0)
    case NumInt(l, _) => LongPoint.newExactQuery(index.name, l)
    case NumDec(bd, _) => DoublePoint.newExactQuery(index.name, bd.toDouble)
    case json => throw new RuntimeException(s"Unsupported equality check: $json (${index.rw.definition})")
  }

  override def doSearch[V](query: Query[D, M],
                           transaction: Transaction[D],
                           conversion: Conversion[V]): IO[SearchResults[D, V]] = IO.blocking {
    val q: LuceneQuery = filter2Lucene(query.filter)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = getIndexSearcher(transaction)
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
      case m: Conversion.Materialized => fs2.Stream.fromBlockingIterator[IO](scoreDocs.iterator.map { scoreDoc =>
        val json = obj(m.indexes.toList.map { index =>
          val s = storedFields.document(scoreDoc.doc).get(index.name)
          index.name -> Index.string2Json(s)(index.rw)
        }: _*)
        val score = scoreDoc.score.toDouble
        Materialized[D](json) -> score
      }, 512)
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

  private def getIndexSearcher(implicit transaction: Transaction[D]): IndexSearcher =
    transaction.getOrCreate(indexSearcherKey, searcherManager.acquire())

  override def aggregate(query: AggregateQuery[D, M])
                        (implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] =
    throw new UnsupportedOperationException("Aggregate functions not supported in Lucene currently")

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking {
    val indexSearcher = getIndexSearcher
    indexSearcher.count(new MatchAllDocsQuery)
  }

  private def sort2SortField(sort: Sort): SortField = sort match {
    case Sort.BestMatch => SortField.FIELD_SCORE
    case Sort.IndexOrder => SortField.FIELD_DOC
    case Sort.ByIndex(field, dir) =>
      val sortType = field.rw.definition match {
        case DefType.Str => SortField.Type.STRING
        case DefType.Dec => SortField.Type.DOUBLE
        case DefType.Int => SortField.Type.LONG
        case _ => throw new RuntimeException(s"Unsupported sort type for ${field.rw.definition}")
      }
      field.rw.definition match {
        case DefType.Int => new SortedNumericSortField(field.name, sortType, dir == SortDirection.Descending)
        case DefType.Str => new SortField(field.name, sortType, dir == SortDirection.Descending)
        case d => throw new RuntimeException(s"Unsupported sort definition: $d")
      }
    case Sort.ByDistance(index, from) =>
      val separate = index.rw.definition.className.collect {
        case "lightdb.spatial.GeoPoint" => true
      }.getOrElse(false)
      val fieldSortName = if (separate) s"${index.name}Sort" else index.name
      LatLonDocValuesField.newDistanceSort(fieldSortName, from.latitude, from.longitude)
  }
}