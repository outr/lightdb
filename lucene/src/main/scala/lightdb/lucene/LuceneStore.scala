package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.{Asable, Convertible}
import lightdb.SortDirection.Ascending
import lightdb.aggregate.{AggregateQuery, AggregateType}
import lightdb.collection.Collection
import lightdb.{Field, Id, LightDB, Query, SearchResults, Sort, SortDirection, Tokenized, UniqueIndex}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.{Condition, Filter}
import lightdb.lucene.index.Index
import lightdb.materialized.{MaterializedAggregate, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo, Spatial}
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.util.Aggregator
import org.apache.lucene.document.{DoubleField, DoublePoint, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LongField, LongPoint, NumericDocValuesField, SortedDocValuesField, SortedNumericDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.search.{BooleanClause, BooleanQuery, BoostQuery, FieldExistsQuery, IndexSearcher, MatchAllDocsQuery, RegexpQuery, ScoreDoc, SearcherFactory, SearcherManager, SortField, SortedNumericSortField, TermQuery, TopFieldCollector, TopFieldCollectorManager, TopFieldDocs, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.index.{StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.BytesRef

import java.nio.file.Path
import scala.language.implicitConversions

class LuceneStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](directory: Option[Path], val storeMode: StoreMode) extends Store[Doc, Model] {
  private lazy val index = Index(directory)

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = transaction.put(
    key = StateKey[Doc],
    value = LuceneState[Doc](index)
  )

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val luceneFields = fields.flatMap { field =>
      createLuceneFields(field, doc)
    }
    addDoc(id(doc), luceneFields, upsert = false)
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val luceneFields = fields.flatMap { field =>
      createLuceneFields(field, doc)
    }
    addDoc(id(doc), luceneFields, upsert = true)
  }

  private def createLuceneFields(field: Field[Doc, _], doc: Doc): List[LuceneField] = {
    def fs: LuceneField.Store = if (storeMode == StoreMode.All || field.indexed) LuceneField.Store.YES else LuceneField.Store.NO
    val json = field.getJson(doc)
    var fields = List.empty[LuceneField]
    def add(field: LuceneField): Unit = fields = field :: fields
    field match {
      case t: Tokenized[Doc] =>
        List(new LuceneField(field.name, t.get(doc), if (fs == LuceneField.Store.YES) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED))
      case _ =>
        def addJson(json: Json, d: DefType): Unit = d match {
          case DefType.Opt(DefType.Obj(_, Some("lightdb.spatial.Geo.Point"))) => json match {
            case Null => // Don't set anything
            case _ =>
              val p = json.as[Geo.Point]
              add(new LatLonPoint(field.name, p.latitude, p.longitude))
              add(new StoredField(field.name, JsonFormatter.Compact(p.json)))
          }
          case DefType.Obj(_, Some("lightdb.spatial.Geo.Point")) =>
            val p = json.as[Geo.Point]
            add(new LatLonPoint(field.name, p.latitude, p.longitude))
            add(new StoredField(field.name, JsonFormatter.Compact(p.json)))
          case DefType.Str => json match {
            case Null => add(new StringField(field.name, Field.NullString, fs))
            case _ => add(new StringField(field.name, json.asString, fs))
          }
          case DefType.Json | DefType.Obj(_, _) => add(new StringField(field.name, JsonFormatter.Compact(json), fs))
          case DefType.Opt(d) => addJson(json, d)
          case DefType.Arr(d) => json.asVector.foreach(json => addJson(json, d))
          case DefType.Bool => add(new IntField(field.name, if (json.asBoolean) 1 else 0, fs))
          case DefType.Int => add(new LongField(field.name, json.asLong, fs))
          case DefType.Dec => add(new DoubleField(field.name, json.asDouble, fs))
          case _ => throw new UnsupportedOperationException(s"Unsupported definition (${field.name}): $d for $json")
        }
        addJson(json, field.rw.definition)

        val fieldSortName = s"${field.name}Sort"
        field.getJson(doc) match {
          case Str(s, _) =>
            val bytes = new BytesRef(s)
            val sorted = new SortedDocValuesField(fieldSortName, bytes)
            add(sorted)
          case NumInt(l, _) => add(new NumericDocValuesField(fieldSortName, l))
          case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
            case Geo.Point(latitude, longitude) => add(new LatLonDocValuesField(fieldSortName, latitude, longitude))
            case _ => // Ignore
          }
          case _ => // Ignore
        }
        fields
    }
  }

  private def addDoc(id: Id[Doc], fields: List[LuceneField], upsert: Boolean): Unit = if (fields.tail.nonEmpty) {
    val document = new LuceneDocument
    fields.foreach(document.add)

    if (upsert) {
      index.indexWriter.updateDocument(new Term("_id", id.value), document)
    } else {
      index.indexWriter.addDocument(document)
    }
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean = get(idField, id).nonEmpty

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    val filter = Filter.Equals(field, value)
    val query = Query[Doc, Model](collection, filter = Some(filter), limit = Some(1))
    doSearch[Doc](query, Conversion.Doc()).list.headOption
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    val query = filter2Lucene(Some(field === value))
    index.indexWriter.deleteDocuments(query)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Int =
    state.indexSearcher.count(new MatchAllDocsQuery)

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] =
    doSearch[Doc](Query[Doc, Model](collection), Conversion.Doc()).iterator

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {
    val q: LuceneQuery = filter2Lucene(query.filter)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = state.indexSearcher
    val limit = query.limit.map(l => math.min(l, 100)).getOrElse(100) + query.offset
    if (limit <= 0) throw new RuntimeException(s"Limit must be a positive value, but set to $limit")
    def search(total: Option[Int]): TopFieldDocs = {
      val topFieldDocs = indexSearcher.search(q, total.getOrElse(limit), s, query.scoreDocs)
//      val collectorManager = new TopFieldCollectorManager(s, total.getOrElse(query.offset + limit), null, Int.MaxValue, false)
//      val topFieldDocs: TopFieldDocs = indexSearcher.search(q, collectorManager)
      val totalHits = total.getOrElse(topFieldDocs.totalHits.value.toInt)
      if (totalHits > topFieldDocs.scoreDocs.length && total.isEmpty && query.limit.forall(l => l + query.offset > limit)) {
        search(Some(query.limit.map(l => math.min(l, totalHits)).getOrElse(totalHits)))
      } else {
        topFieldDocs
      }
    }
    val topFieldDocs: TopFieldDocs = search(None)
    val scoreDocs: List[ScoreDoc] = {
      val list = topFieldDocs
        .scoreDocs
        .toList
        .drop(query.offset)
      query.minDocScore match {
        case Some(min) => list.filter(_.score.toDouble >= min)
        case None => list
      }
    }
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val idsAndScores = scoreDocs.map(doc => Id[Doc](storedFields.document(doc.doc).get("_id")) -> doc.score.toDouble)
    def json[F](scoreDoc: ScoreDoc, field: Field[Doc, F]): Json = {
      val s = storedFields.document(scoreDoc.doc).get(field.name)
      Field.string2Json(field.name, s, field.rw.definition)
    }
    def value[F](scoreDoc: ScoreDoc, field: Field[Doc, F]): F = json[F](scoreDoc, field).as[F](field.rw)
    def docIterator(): Iterator[(Doc, Double)] = if (storeMode == StoreMode.All) {
      val i = scoreDocs.iterator
      val docIterator = collection.model match {
        case c: JsonConversion[Doc] => i.map { scoreDoc =>
          val o = obj(fields.map(f => f.name -> json(scoreDoc, f)): _*)
          c.convertFromJson(o) -> scoreDoc.score.toDouble
        }
        case _ => i.map { scoreDoc =>
          val map = fields.map { field =>
            field.name -> value(scoreDoc, field)
          }.toMap
          collection.model.map2Doc(map) -> scoreDoc.score.toDouble
        }
      }
      docIterator
    } else {
      idsAndScores.iterator.flatMap {
        case (id, score) => collection.get(id)(transaction).map(doc => doc -> score)
      }
    }
    def jsonIterator(fields: List[Field[Doc, _]]): Iterator[(Json, Double)] = {
      scoreDocs.iterator.map { scoreDoc =>
        val json = obj(fields.map { field =>
          val s = storedFields.document(scoreDoc.doc).get(field.name)
          field.name -> Field.string2Json(field.name, s, field.rw.definition)
        }: _*)
        val score = scoreDoc.score.toDouble
        json -> score
      }
    }
    val iterator: Iterator[(V, Double)] = conversion match {
      case Conversion.Value(field) => scoreDocs.iterator.map { scoreDoc =>
        value(scoreDoc, field) -> scoreDoc.score.toDouble
      }
      case Conversion.Doc() => docIterator().asInstanceOf[Iterator[(V, Double)]]
      case Conversion.Converted(c) => docIterator().map {
        case (doc, score) => c(doc) -> score
      }
      case Conversion.Materialized(fields) => jsonIterator(fields).map {
        case (json, score) => MaterializedIndex[Doc, Model](json, collection.model).asInstanceOf[V] -> score
      }
      case Conversion.Json(fields) => jsonIterator(fields).asInstanceOf[Iterator[(V, Double)]]
      case m: Conversion.Distance[Doc] => idsAndScores.iterator.map {
        case (id, score) =>
          val doc = collection(id)(transaction)
          val distance = m.field.get(doc).map(d => Spatial.distance(m.from, d))
          DistanceAndDoc(doc, distance) -> score
      }
    }
    SearchResults(
      offset = query.offset,
      limit = query.limit,
      total = Some(total),
      iteratorWithScore = iterator,
      transaction = transaction
    )
  }

  private def filter2Lucene(filter: Option[Filter[Doc]]): LuceneQuery = filter match {
    case Some(f) => f match {
      case f: Filter.Equals[Doc, _] => exactQuery(f.field(collection.model), f.getJson(collection.model))
      case f: Filter.NotEquals[Doc, _] =>
        val b = new BooleanQuery.Builder
        b.add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
        b.add(exactQuery(f.field(collection.model), f.getJson(collection.model)), BooleanClause.Occur.MUST_NOT)
        b.build()
      case f: Filter.Regex[Doc, _] => new RegexpQuery(new Term(f.fieldName, f.expression))
      case f: Filter.In[Doc, _] =>
        val queries = f.getJson(collection.model).map(json => exactQuery(f.field(collection.model), json))
        val b = new BooleanQuery.Builder
        b.setMinimumNumberShouldMatch(1)
        queries.foreach { q =>
          b.add(q, BooleanClause.Occur.SHOULD)
        }
        b.build()
      case Filter.RangeLong(fieldName, from, to) => LongField.newRangeQuery(fieldName, from.getOrElse(Long.MinValue), to.getOrElse(Long.MaxValue))
      case Filter.RangeDouble(fieldName, from, to) => DoubleField.newRangeQuery(fieldName, from.getOrElse(Double.MinValue), to.getOrElse(Double.MaxValue))
      case Filter.Parsed(fieldName, query, allowLeadingWildcard) =>
        val parser = new QueryParser(fieldName, this.index.analyzer)
        parser.setAllowLeadingWildcard(allowLeadingWildcard)
        parser.setSplitOnWhitespace(true)
        parser.parse(query)
      case Filter.Distance(fieldName, from, radius) =>
        LatLonPoint.newDistanceQuery(fieldName, from.latitude, from.longitude, radius.toMeters)
      case Filter.Multi(minShould, clauses) =>
        val b = new BooleanQuery.Builder
        val hasShould = clauses.exists(c => c.condition == Condition.Should || c.condition == Condition.Filter)
        val minShouldMatch = if (hasShould) minShould else 0
        b.setMinimumNumberShouldMatch(minShouldMatch)
        clauses.foreach { c =>
          val q = filter2Lucene(Some(c.filter))
          val query = c.boost match {
            case Some(boost) => new BoostQuery(q, boost.toFloat)
            case None => q
          }
          val occur = c.condition match {
            case Condition.Must => BooleanClause.Occur.MUST
            case Condition.MustNot => BooleanClause.Occur.MUST_NOT
            case Condition.Filter => BooleanClause.Occur.FILTER
            case Condition.Should => BooleanClause.Occur.SHOULD
          }
          b.add(query, occur)
        }
        if (minShouldMatch == 0 && !clauses.exists(_.condition == Condition.Must)) {
          b.add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
        }
        b.build()
    }
    case None => new MatchAllDocsQuery
  }

  private def exactQuery(field: Field[Doc, _], json: Json): LuceneQuery = json match {
    case Str(s, _) if field.isInstanceOf[Tokenized[_]] =>
      val b = new BooleanQuery.Builder
      s.split("\\s+").foreach(s => b.add(new TermQuery(new Term(field.name, s)), BooleanClause.Occur.MUST))
      b.build()
    case Str(s, _) => new TermQuery(new Term(field.name, s))
    case Bool(b, _) => IntPoint.newExactQuery(field.name, if (b) 1 else 0)
    case NumInt(l, _) => LongPoint.newExactQuery(field.name, l)
    case NumDec(bd, _) => DoublePoint.newExactQuery(field.name, bd.toDouble)
    case Arr(v, _) =>
      val b = new BooleanQuery.Builder
      v.foreach { json =>
        val q = exactQuery(field, json)
        b.add(q, BooleanClause.Occur.MUST)
      }
      b.build()
    case Null => field.rw.definition match {
      case DefType.Opt(DefType.Str) => new TermQuery(new Term(field.name, Field.NullString))
      case _ => new TermQuery(new Term(field.name, "null"))
    }
    case json => throw new RuntimeException(s"Unsupported equality check: $json (${field.rw.definition})")
  }

  private def sort2SortField(sort: Sort): SortField = {
    sort match {
      case Sort.BestMatch => SortField.FIELD_SCORE
      case Sort.IndexOrder => SortField.FIELD_DOC
      case Sort.ByField(field, dir) =>
        val fieldSortName = s"${field.name}Sort"
        def st(d: DefType): SortField.Type = d match {
          case DefType.Str => SortField.Type.STRING
          case DefType.Dec => SortField.Type.DOUBLE
          case DefType.Int => SortField.Type.LONG
          case DefType.Opt(t) => st(t)
          case _ => throw new RuntimeException(s"Unsupported sort type for ${field.rw.definition}")
        }
        val sortType = st(field.rw.definition)
        def sf(d: DefType): SortField = d match {
          case DefType.Int => new SortedNumericSortField(fieldSortName, sortType, dir == SortDirection.Descending)
          case DefType.Str => new SortField(fieldSortName, sortType, dir == SortDirection.Descending)
          case DefType.Opt(t) => sf(t)
          case d => throw new RuntimeException(s"Unsupported sort definition: $d")
        }
        sf(field.rw.definition)
      case Sort.ByDistance(field, from, _) =>
        val fieldSortName = s"${field.name}Sort"
        LatLonDocValuesField.newDistanceSort(fieldSortName, from.latitude, from.longitude)
    }
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, collection)

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int =
    aggregate(query).length

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val count = this.count
    index.indexWriter.deleteAll()
    count
  }

  override def dispose(): Unit = {
    index.indexWriter.flush()
    index.indexWriter.commit()
    index.indexWriter.close()
  }
}

object LuceneStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB, name: String, storeMode: StoreMode): Store[Doc, Model] =
    new LuceneStore[Doc, Model](db.directory.map(_.resolve(s"$name.lucene")), storeMode)
}