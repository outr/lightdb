package lightdb.index.lucene

import cats.effect.IO
import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.collection.Collection
import lightdb.index.{Indexer, SearchResult, SearchResults}
import lightdb.query.{Condition, Filter, Query}
import lightdb.{Document, Id}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, IndexSearcher, MatchAllDocsQuery, PhraseQuery, ScoreDoc, SearcherFactory, SearcherManager, Sort, SortField, TermQuery, Query => LuceneQuery}
import org.apache.lucene.store.{ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.document.{DoubleField, Field, FloatField, IntField, IntRange, LongField, StringField, TextField, Document => LuceneDocument}

import java.nio.file.Path
import scala.collection.immutable.ArraySeq

case class LuceneIndexer[D <: Document[D]](collection: Collection[D],
                                           autoCommit: Boolean = false,
                                           analyzer: Analyzer = new StandardAnalyzer) extends Indexer[D] { i =>
  private var disposed = false
  private lazy val path: Option[Path] = collection.db.directory.map(_.resolve(collection.collectionName))
  private lazy val directory = path
    .map(p => FSDirectory.open(p))
    .getOrElse(new ByteBuffersDirectory())

  private lazy val config = new IndexWriterConfig(analyzer)
  private lazy val indexWriter = new IndexWriter(directory, config)

  private var _indexSearcher: IndexSearcher = _
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val parser = new QueryParser("_id", analyzer)

  private def withIndexSearcher[Return](f: IndexSearcher => Return) = synchronized {
    if (_indexSearcher == null) {
      _indexSearcher = searcherManager.acquire()
    }
    f(_indexSearcher)
  }

  private def addField(document: LuceneDocument, field: lightdb.field.Field[D, _], value: Any): Unit = {
    def textFieldType = if (field.stored) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED
    def fieldStore: Field.Store = if (field.stored) Field.Store.YES else Field.Store.NO
    value match {
      case id: Id[_] => document.add(new StringField(field.name, id.value, fieldStore))
      case s: String => document.add(new Field(field.name, s, textFieldType))
      case i: Int => document.add(new IntField(field.name, i, fieldStore))
      case l: Long => document.add(new LongField(field.name, l, fieldStore))
      case f: Float => document.add(new FloatField(field.name, f, fieldStore))
      case d: Double => document.add(new DoubleField(field.name, d, fieldStore))
      case bd: BigDecimal => document.add(new StringField(field.name, bd.asString, fieldStore))
      case Str(s, _) => addField(document, field, s)          // TODO: Should this store as a StringField instead? Maybe add something to field for tokenize?
      case NumDec(bd, _) => addField(document, field, bd)
      case NumInt(i, _) => addField(document, field, i)
      case obj: Obj =>
        val jsonString = JsonFormatter.Compact(obj)
        document.add(new StringField(field.name, jsonString, fieldStore))
      case null | Null => // Ignore null
      case value =>
        val json = field.rw.asInstanceOf[RW[Any]].read(value)
        addField(document, field, json)
    }
  }

  override def put(value: D): IO[D] = IO {
    val document = new LuceneDocument
    collection.mapping.fields.foreach { field =>
      addField(document, field, field.getter(value))
    }
    if (document.iterator().hasNext) {
      indexWriter.updateDocument(new Term("_id", value._id.value), document)
    }
    doAutoCommit()
    value
  }

  override def delete(id: Id[D]): IO[Unit] = IO {
    indexWriter.deleteDocuments(parser.parse(s"_id:${id.value}"))
    doAutoCommit()
  }

  private def doAutoCommit(): Unit = if (autoCommit) {
    commitBlocking()
  }

  private def commitBlocking(): Unit = {
    indexWriter.flush()
    indexWriter.commit()
    i.synchronized {
      if (_indexSearcher != null) {
        searcherManager.release(_indexSearcher)
      }
      _indexSearcher = null
    }
    searcherManager.maybeRefreshBlocking()
  }

  override def commit(): IO[Unit] = IO(commitBlocking())

  override def count(): IO[Int] = IO {
    withIndexSearcher(_.count(new MatchAllDocsQuery))
  }

  private def condition2Lucene(condition: Condition): BooleanClause.Occur = condition match {
    case Condition.Filter => BooleanClause.Occur.FILTER
    case Condition.Must => BooleanClause.Occur.MUST
    case Condition.MustNot => BooleanClause.Occur.MUST_NOT
    case Condition.Should => BooleanClause.Occur.SHOULD
  }

  private def filter2Query(filter: Filter[D]): LuceneQuery = filter match {
    case Filter.ParsableSearchTerm(query, allowLeadingWildcard) =>
      parser.setAllowLeadingWildcard(allowLeadingWildcard)
      parser.parse(query)
    case Filter.GroupedFilter(minimumNumberShouldMatch, filters) =>
      val b = new BooleanQuery.Builder
      b.setMinimumNumberShouldMatch(minimumNumberShouldMatch)
      if (filters.forall(_._2 == Condition.MustNot)) {           // Work-around for all negative groups, something must match
        b.add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
      }
      filters.foreach {
        case (filter, condition) => b.add(filter2Query(filter), condition2Lucene(condition))
      }
      b.build()
    case Filter.Equals(field, value) => value match {
      case s: String =>
        val b = new PhraseQuery.Builder
        s.toLowerCase.split(' ').foreach { word =>
          b.add(new Term(field.name, word))
        }
        b.setSlop(0)
        b.build()
      case i: Int => IntField.newExactQuery(field.name, i)
      case id: Id[_] => new TermQuery(new Term(field.name, id.value))
      case json: Json => new TermQuery(new Term(field.name, JsonFormatter.Compact(json)))
      case _ => throw new UnsupportedOperationException(s"Unsupported: $value (${field.name})")
    }
    case Filter.NotEquals(field, value) =>
      filter2Query(Filter.GroupedFilter(0, List(Filter.Equals(field, value) -> Condition.MustNot)))
    case Filter.Includes(field, values) =>
      filter2Query(Filter.GroupedFilter(1, values.map(v => Filter.Equals(field, v) -> Condition.Should).toList))
    case Filter.Excludes(field, values) =>
      filter2Query(Filter.GroupedFilter(0, values.map(v => Filter.Equals(field, v) -> Condition.MustNot).toList))
    case _ => throw new RuntimeException(s"Unsupported filter: $filter")
  }

  override def search(query: Query[D]): IO[SearchResults[D]] = IO {
    val q = query.filter.map(filter2Query).getOrElse(new MatchAllDocsQuery)
    val sortFields = if (query.sort.isEmpty) {
      List(SortField.FIELD_SCORE)
    } else {
      query.sort.map {
        case lightdb.query.Sort.BestMatch => SortField.FIELD_SCORE
        case lightdb.query.Sort.IndexOrder => SortField.FIELD_DOC
        case lightdb.query.Sort.ByField(field, reverse) =>
          val sortType = field.rw.definition match {
            case DefType.Str => SortField.Type.STRING
            case d => throw new RuntimeException(s"Unsupported DefType: $d")
          }
          new SortField(field.name, sortType, reverse)
      }
    }
    // TODO: Offset
    val (hits, storedFields, total) = withIndexSearcher { indexSearcher =>
      val topDocs = indexSearcher.search(q, query.limit, new Sort(sortFields: _*), query.scoreDocs)
      val hits = topDocs.scoreDocs
      val total = topDocs.totalHits.value.toInt
      val storedFields = indexSearcher.storedFields()
      (hits, storedFields, total)
    }
    val stream = fs2.Stream[IO, ScoreDoc](ArraySeq.unsafeWrapArray(hits): _*)
      .map { sd =>
        LuceneSearchResult(sd, collection, storedFields)
      }
    SearchResults(query, total, stream)
  }

  override def truncate(): IO[Unit] = IO {
    indexWriter.deleteAll()
    doAutoCommit()
  }

  def close(): IO[Unit] = IO {
    searcherManager.close()
    indexWriter.flush()
    indexWriter.commit()
    indexWriter.close()
    if (_indexSearcher != null) {
      searcherManager.release(_indexSearcher)
    }
  }

  override def dispose(): IO[Unit] = close().map { _ =>
    disposed = true
  }
}
