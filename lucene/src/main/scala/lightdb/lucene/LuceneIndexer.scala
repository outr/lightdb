package lightdb.lucene

import fabric._
import fabric.define.DefType
import lightdb.index.{Index, Indexer, MaterializedAggregate, MaterializedIndex}
import lightdb.Id
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher, MatchAllDocsQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, SortedNumericSortField, TermQuery, TopFieldCollector, TopFieldCollectorManager, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{DoubleField, DoublePoint, Field, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LongField, LongPoint, StringField, TextField, Document => LuceneDocument, Field => LuceneField}

import java.nio.file.{Files, Path}
import lightdb.aggregate.AggregateQuery
import lightdb.document.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.query.{Query, SearchResults, Sort, SortDirection}
import lightdb.spatial.{DistanceAndDoc, DistanceCalculator, GeoPoint}
import lightdb.transaction.{Transaction, TransactionKey}

case class LuceneIndexer[D <: Document[D], M <: DocumentModel[D]](persistent: Boolean = true,
                                                                  analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D, M] {
  private lazy val indexSearcherKey: TransactionKey[IndexSearcher] = TransactionKey("indexSearcher")

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
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(1_000)     // TODO: Make configurable
    c
  }
  private lazy val indexWriter = new IndexWriter(directory, config)

  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val parser = new QueryParser("_id", analyzer)

  override def transactionEnd(transaction: Transaction[D]): Unit = {
    super.transactionEnd(transaction)
    transaction.get(indexSearcherKey).foreach { indexSearcher =>
      searcherManager.release(indexSearcher)
    }
  }

  override def postSet(doc: D, transaction: Transaction[D]): Unit = {
    super.postSet(doc, transaction)
    indexDoc(doc, indexes)
  }

  override def postDelete(doc: D, transaction: Transaction[D]): Unit = {
    super.postDelete(doc, transaction)
    delete(doc._id)
  }

  override def commit(transaction: Transaction[D]): Unit = {
    super.commit(transaction)
    transaction.get(indexSearcherKey).foreach { _ =>
      commitBlocking()
    }
  }

  override def truncate(transaction: Transaction[D]): Unit = {
    super.truncate(transaction)
    truncate()
  }

  private def indexDoc(doc: D, indexes: List[Index[_, D]]): Unit = {
    val fields = indexes.flatMap { index =>
      createFields(index, doc)
    }
    addDoc(doc._id, fields)
  }

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

  private[lightdb] def addDoc(id: Id[D], fields: List[LuceneField]): Unit = if (fields.tail.nonEmpty) {
    val document = new LuceneDocument
    fields.foreach(document.add)
    indexWriter.updateDocument(new Term("_id", id.value), document)
  }

  private[lightdb] def delete(id: Id[D]): Unit = indexWriter.deleteDocuments(parser.parse(s"_id:${id.value}"))

  def truncate(): Unit = indexWriter.deleteAll()

  private def commitBlocking(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
  }

  private def filter2Lucene(filter: Option[Filter[D]]): LuceneQuery = filter match {
    case Some(f) => f match {
      case f: Filter.Equals[_, D] => exactQuery(f.index, f.getJson)
      case f: Filter.In[_, D] =>
        val queries = f.getJson.map(json => exactQuery(f.index, json))
        val b = new BooleanQuery.Builder
        b.setMinimumNumberShouldMatch(1)
        queries.foreach { q =>
          b.add(q, BooleanClause.Occur.SHOULD)
        }
        b.build()
      case Filter.Combined(filters) =>
        val queries = filters.map(f => filter2Lucene(Some(f)))
        val b = new BooleanQuery.Builder
        b.setMinimumNumberShouldMatch(1)
        queries.foreach { q =>
          b.add(q, BooleanClause.Occur.SHOULD)
        }
        b.build()
      case Filter.RangeLong(index, from, to) => LongField.newRangeQuery(index.name, from.getOrElse(Long.MinValue), to.getOrElse(Long.MaxValue))
      case Filter.RangeDouble(index, from, to) => DoubleField.newRangeQuery(index.name, from.getOrElse(Double.MinValue), to.getOrElse(Double.MaxValue))
      case Filter.Parsed(index, query, allowLeadingWildcard) =>
        val parser = new QueryParser(index.name, analyzer)
        parser.setAllowLeadingWildcard(allowLeadingWildcard)
        parser.setSplitOnWhitespace(true)
        parser.parse(query)
      case Filter.Distance(index, from, radius) =>
        LatLonPoint.newDistanceQuery(index.name, from.latitude, from.longitude, radius.toMeters)
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

  private def createIterator[V](query: Query[D, M],
                              transaction: Transaction[D],
                              conversion: Conversion[V]): (Iterator[(V, Double)], Int) = {
    val q: LuceneQuery = filter2Lucene(query.filter)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = getIndexSearcher(transaction)
    val limit = query.limit.map(l => math.min(l - query.offset, query.batchSize)).getOrElse(query.batchSize)
    val collectorManager = new TopFieldCollectorManager(s, query.offset + limit, null, Int.MaxValue, false)
    val topFieldDocs: TopFieldDocs = indexSearcher.search(q, collectorManager)
    val scoreDocs: List[ScoreDoc] = topFieldDocs
      .scoreDocs
      .toList
      .slice(query.offset, query.offset + limit)
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val idsAndScores = scoreDocs.map(doc => Id[D](storedFields.document(doc.doc).get("_id")) -> doc.score.toDouble)
    val iterator: Iterator[(V, Double)] = conversion match {
      case Conversion.Id => idsAndScores.iterator
      case Conversion.Doc => idsAndScores.iterator.flatMap {
        case (id, score) => collection.get(id)(transaction).map(doc => doc -> score)
      }
      case m: Conversion.Materialized => scoreDocs.iterator.map { scoreDoc =>
        val json = obj(m.indexes.map { index =>
          val s = storedFields.document(scoreDoc.doc).get(index.name)
          index.name -> Index.string2Json(s)(index.rw)
        }: _*)
        val score = scoreDoc.score.toDouble
        MaterializedIndex[D, M](json, collection.model) -> score
      }
      case m: Conversion.Distance => idsAndScores.iterator.map {
        case (id, score) =>
          val doc = collection(id)(transaction)
          val distance = DistanceCalculator(m.from, m.index.get(doc).head)
          DistanceAndDoc(doc, distance) -> score
      }
    }
    (iterator, total)
  }

  override def doSearch[V](query: Query[D, M],
                           transaction: Transaction[D],
                           conversion: Conversion[V]): SearchResults[D, V] =
    createIterator(query, transaction, conversion) match {
      case (page1, total) =>
        val limit = query.limit.map(l => math.min(l, total)).getOrElse(total) - query.offset
        val pages = math.ceil(limit.toDouble / query.batchSize.toDouble).toInt
        val iterator = if (pages > 1) {
//          val streams = (1 until pages).toList.map { page =>
//            fs2.Stream.force(createIterator(query.copy(offset = page * query.batchSize), transaction, conversion)
//              .map(_._1))
//          }
//          streams.foldLeft(page1)((combined, stream) => combined ++ stream)
          // TODO: Implement
          ???
        } else {
          page1
        }
        SearchResults(
          offset = query.offset,
          limit = query.limit,
          total = Some(total),
          scoredIterator = iterator,
          transaction = transaction
        )
    }

  private def getIndexSearcher(implicit transaction: Transaction[D]): IndexSearcher = {
    transaction.getOrCreate(indexSearcherKey, {
      searcherManager.maybeRefreshBlocking()
      searcherManager.acquire()
    })
  }

  override def aggregate(query: AggregateQuery[D, M])
                        (implicit transaction: Transaction[D]): Iterator[MaterializedAggregate[D, M]] =
    throw new UnsupportedOperationException("Aggregate functions not supported in Lucene currently")

  override def count(implicit transaction: Transaction[D]): Int = {
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