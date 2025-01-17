package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{Condition, Filter}
import lightdb.lucene.index.Index
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo, Spatial}
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.util.Aggregator
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, DoublePoint, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, LongPoint, NumericDocValuesField, SortedDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.{DrillDownQuery, FacetsCollector, FacetsCollectorManager, FacetsConfig, FacetField => LuceneFacetField}
import org.apache.lucene.geo.{Line, Polygon}
import org.apache.lucene.index.{DirectoryReader, SegmentReader, StoredFields, Term}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, BoostQuery, IndexSearcher, MatchAllDocsQuery, MultiCollectorManager, RegexpQuery, ScoreDoc, SortField, SortedNumericSortField, TermQuery, TopFieldCollectorManager, TopFieldDocs, TotalHitCountCollectorManager, WildcardQuery, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.{BytesRef, Version}
import rapid._

import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LuceneStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     model: Model,
                                                                     directory: Option[Path],
                                                                     val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  IndexSearcher.setMaxClauseCount(10_000_000)

  private lazy val index = Index(directory)
  private lazy val facetsConfig: FacetsConfig = {
    val c = new FacetsConfig
    fields.foreach {
      case ff: FacetField[_] =>
        if (ff.hierarchical) c.setHierarchical(ff.name, ff.hierarchical)
        if (ff.multiValued) c.setMultiValued(ff.name, ff.multiValued)
        if (ff.requireDimCount) c.setRequireDimCount(ff.name, ff.requireDimCount)
      case _ => // Ignore
    }
    c
  }
  private lazy val hasFacets: Boolean = fields.exists(_.isInstanceOf[FacetField[_]])
  private def facetsPrepareDoc(doc: LuceneDocument): LuceneDocument = if (hasFacets) {
    facetsConfig.build(index.taxonomyWriter, doc)
  } else {
    doc
  }

  directory.foreach { path =>
    if (Files.exists(path)) {
      val directory = FSDirectory.open(path)
      val reader = DirectoryReader.open(directory)
      reader.leaves().forEach { leaf =>
        val dataVersion = leaf.reader().asInstanceOf[SegmentReader].getSegmentInfo.info.getVersion
        val latest = Version.LATEST
        if (latest != dataVersion) {
          // TODO: Support re-indexing
          scribe.warn(s"Data Version: $dataVersion, Latest Version: $latest")
        }
      }
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.put(
      key = StateKey[Doc],
      value = LuceneState[Doc](index, hasFacets)
    )
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] =
    addDoc(doc, upsert = false)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] =
    addDoc(doc, upsert = true)

  private def createGeoFields(field: Field[Doc, _],
                              json: Json,
                              add: LuceneField => Unit): Unit = {
    field.className match {
      case _ =>
        def indexPoint(p: Geo.Point): Unit = try {
          LatLonShape.createIndexableFields(field.name, p.latitude, p.longitude)
        } catch {
          case t: Throwable => throw new RuntimeException(s"Failed to add LatLonPoint.createIndexableFields(${field.name}, ${p.latitude}, ${p.longitude}): ${JsonFormatter.Default(json)}", t)
        }
        def indexLine(l: Geo.Line): Unit = {
          val line = new Line(l.points.map(_.latitude).toArray, l.points.map(_.longitude).toArray)
          LatLonShape.createIndexableFields(field.name, line)
        }
        def indexPolygon(p: Geo.Polygon): Unit = {
          def convert(p: Geo.Polygon): Polygon =
            new Polygon(p.points.map(_.latitude).toArray, p.points.map(_.longitude).toArray)
          val polygon = convert(p)
          LatLonShape.createIndexableFields(field.name, polygon)
        }
        def indexGeo(geo: Geo): Unit = geo match {
          case p: Geo.Point => indexPoint(p)
          case Geo.MultiPoint(points) => points.foreach(indexPoint)
          case l: Geo.Line => indexLine(l)
          case Geo.MultiLine(lines) => lines.foreach(indexLine)
          case p: Geo.Polygon => indexPolygon(p)
          case Geo.MultiPolygon(polygons) => polygons.foreach(indexPolygon)
          case Geo.GeometryCollection(geometries) => geometries.foreach(indexGeo)
        }
        val list = json match {
          case Arr(value, _) => value.toList.map(_.as[Geo])
          case _ => List(json.as[Geo])
        }
        list.foreach { geo =>
          indexGeo(geo)
          add(new LatLonPoint(field.name, geo.center.latitude, geo.center.longitude))
        }
        if (list.isEmpty) {
          add(new LatLonPoint(field.name, 0.0, 0.0))
        }
    }
    add(new StoredField(field.name, JsonFormatter.Compact(json)))
  }

  private def createLuceneFields(field: Field[Doc, _], doc: Doc, state: IndexingState): List[LuceneField] = {
    def fs: LuceneField.Store = if (storeMode.isAll || field.indexed) LuceneField.Store.YES else LuceneField.Store.NO
    val json = field.getJson(doc, state)
    var fields = List.empty[LuceneField]
    def add(field: LuceneField): Unit = fields = field :: fields
    field match {
      case ff: FacetField[Doc] => ff.get(doc, ff, state).flatMap { value =>
        if (value.path.nonEmpty || ff.hierarchical) {
          val path = if (ff.hierarchical) value.path ::: List("$ROOT$") else value.path
          Some(new LuceneFacetField(field.name, path: _*))
        } else {
          None
        }
      }
      case t: Tokenized[Doc] =>
        List(new LuceneField(field.name, t.get(doc, t, state), if (fs == LuceneField.Store.YES) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED))
      case _ =>
        def addJson(json: Json, d: DefType): Unit = {
          if (field.isSpatial) {
            if (json != Null) try {
              createGeoFields(field, json, add)
            } catch {
              case t: Throwable => throw new RuntimeException(s"Failure to populate geo field '$name.${field.name}' for $doc (json: $json, className: ${field.className})", t)
            }
          } else {
            d match {
              case DefType.Str => json match {
                case Null => add(new StringField(field.name, Field.NullString, fs))
                case _ => add(new StringField(field.name, json.asString, fs))
              }
              case DefType.Enum(_, _) => add(new StringField(field.name, json.asString, fs))
              case DefType.Opt(d) => addJson(json, d)
              case DefType.Json | DefType.Obj(_, _) => add(new StringField(field.name, JsonFormatter.Compact(json), fs))
              case _ if json == Null => // Ignore null values
              case DefType.Arr(d) =>
                val v = json.asVector
                if (v.isEmpty) {
                  add(new StringField(field.name, "[]", fs))
                } else {
                  v.foreach(json => addJson(json, d))
                }
              case DefType.Bool => add(new IntField(field.name, if (json.asBoolean) 1 else 0, fs))
              case DefType.Int => add(new LongField(field.name, json.asLong, fs))
              case DefType.Dec => add(new DoubleField(field.name, json.asDouble, fs))
              case _ => throw new UnsupportedOperationException(s"Unsupported definition (field: ${field.name}, className: ${field.className}): $d for $json")
            }
          }
        }
        addJson(json, field.rw.definition)

        val fieldSortName = s"${field.name}Sort"
        field.getJson(doc, state) match {
          case Str(s, _) =>
            val bytes = new BytesRef(s)
            val sorted = new SortedDocValuesField(fieldSortName, bytes)
            add(sorted)
          case NumInt(l, _) => add(new NumericDocValuesField(fieldSortName, l))
          case NumDec(d, _) => add(new DoubleDocValuesField(fieldSortName, d.toDouble))
          case j if field.isSpatial && j != Null =>
            val list = j match {
              case Arr(values, _) => values.toList.map(_.as[Geo])
              case _ => List(j.as[Geo])
            }
            list.foreach { g =>
              add(new LatLonDocValuesField(fieldSortName, g.center.latitude, g.center.longitude))
            }
          case _ => // Ignore
        }
        fields
    }
  }

  private def addDoc(doc: Doc, upsert: Boolean): Task[Doc] = Task {
    if (fields.tail.nonEmpty) {
      val id = this.id(doc)
      val state = new IndexingState
      val luceneFields = fields.flatMap { field =>
        createLuceneFields(field, doc, state)
      }
      val document = new LuceneDocument
      luceneFields.foreach(document.add)

      if (upsert) {
        index.indexWriter.updateDocument(new Term("_id", id.value), facetsPrepareDoc(document))
      } else {
        index.indexWriter.addDocument(facetsPrepareDoc(document))
      }
    }
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = get(idField, id).map(_.nonEmpty)

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    val filter = Filter.Equals(field, value)
    val query = Query[Doc, Model, Doc](model, this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
    doSearch[Doc](query).flatMap(_.list).map(_.headOption)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val query = filter2Lucene(Some(field === value))
    index.indexWriter.deleteDocuments(query)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] =
    Task(state.indexSearcher.count(new MatchAllDocsQuery))

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.force(doSearch[Doc](Query[Doc, Model, Doc](model, this, Conversion.Doc())).map(_.stream))

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = Task {
    val q: LuceneQuery = filter2Lucene(query.filter)
    val sortFields = query.sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => query.sort.map(sort2SortField)
    }
    val s = new LuceneSort(sortFields: _*)
    val indexSearcher = state.indexSearcher
    var facetsCollectorManager: Option[FacetsCollectorManager] = None
    val limit = query.limit.map(l => math.min(l, 100)).getOrElse(100) + query.offset
    if (limit <= 0) throw new RuntimeException(s"Limit must be a positive value, but set to $limit")
    var facetResults: Map[FacetField[Doc], FacetResult] = Map.empty
    def search(total: Option[Int]): TopFieldDocs = {
      val hitCountCollectorManager = new TotalHitCountCollectorManager(indexSearcher.getSlices)
      val topFieldCollectorManager = new TopFieldCollectorManager(s, total.getOrElse(limit), Int.MaxValue)
      if (query.facets.nonEmpty) {
        facetsCollectorManager = Some(new FacetsCollectorManager(query.scoreDocs))
      }
      val collectors = List(
        Some(hitCountCollectorManager), Some(topFieldCollectorManager), facetsCollectorManager
      ).flatten
      // TODO: Support exclusion of hitCountCollectorManager if countTotal is false
      val manager = new MultiCollectorManager(collectors: _*)
      val resultCollectors = indexSearcher.search(q, manager).toVector
      val actualCount = resultCollectors(0).asInstanceOf[java.lang.Integer].intValue()
      val topFieldDocs = resultCollectors(1).asInstanceOf[TopFieldDocs]
      val facetsCollector = if (facetsCollectorManager.nonEmpty) Some(resultCollectors(2).asInstanceOf[FacetsCollector]) else None

      facetResults = facetsCollector match {
        case Some(fc) =>
          val facets = new FastTaxonomyFacetCounts(state.taxonomyReader, facetsConfig, fc)
          query.facets.map { fq =>
            Option(fq.childrenLimit match {
              case Some(l) => facets.getTopChildren(l, fq.field.name, fq.path: _*)
              case None => facets.getAllChildren(fq.field.name, fq.path: _*)
            }) match {
              case Some(facetResult) =>
                val values = if (facetResult.childCount > 0) {
                  facetResult.labelValues.toList.map { lv =>
                    FacetResultValue(lv.label, lv.value.intValue())
                  }
                } else {
                  Nil
                }
                val updatedValues = values.filterNot(_.value == "$ROOT$")
                val totalCount = updatedValues.map(_.count).sum
                fq.field -> FacetResult(updatedValues, facetResult.childCount, totalCount)
              case None =>
                fq.field -> FacetResult(Nil, 0, 0)
            }
          }.toMap
        case None => Map.empty
      }
      val totalHits = total.getOrElse(actualCount)
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
        .map { scoreDoc =>
          if (query.scoreDocs) {
            val explanation = indexSearcher.explain(q, scoreDoc.doc)
            // TODO: Add explanation info
            new ScoreDoc(scoreDoc.doc, explanation.getValue.floatValue())
          } else {
            scoreDoc
          }
        }
      query.minDocScore match {
        case Some(min) => list.filter(_.score.toDouble >= min)
        case None => list
      }
    }
    val total: Int = topFieldDocs.totalHits.value.toInt
    val storedFields: StoredFields = indexSearcher.storedFields()
    val idsAndScores = scoreDocs.map(doc => Id[Doc](storedFields.document(doc.doc).get("_id")) -> doc.score.toDouble)
    def jsonField[F](scoreDoc: ScoreDoc, field: Field[Doc, F]): Json = {
      val values = storedFields.document(scoreDoc.doc).getValues(field.name)
        .toVector
        .map(s => Field.string2Json(field.name, s, field.rw.definition))
      if (values.nonEmpty && values.head.isArr) {
        Arr(values.flatMap(_.asVector))
      } else {
        if (values.length > 1) {
          throw new RuntimeException(s"Failure: $values, ${values.head.getClass}")
        }
        values.headOption.getOrElse(Null)
      }
    }
    def value[F](scoreDoc: ScoreDoc, field: Field[Doc, F]): F = jsonField[F](scoreDoc, field).as[F](field.rw)
    def loadScoreDoc(scoreDoc: ScoreDoc): (Doc, Double) = storeMode match {
      case StoreMode.All() =>
        model match {
          case c: JsonConversion[Doc] =>
            val o = obj(fields.map(f => f.name -> jsonField(scoreDoc, f)): _*)
            c.convertFromJson(o) -> scoreDoc.score.toDouble
          case _ =>
            val map = fields.map { field =>
              field.name -> value(scoreDoc, field)
            }.toMap
            model.map2Doc(map) -> scoreDoc.score.toDouble
        }
      case StoreMode.Indexes(storage) =>
        val docId = scoreDoc.doc
        val id = Id[Doc](storedFields.document(docId).get("_id"))
        val score = scoreDoc.score.toDouble
        storage(id).sync() -> score
    }
    def docIterator(): Iterator[(Doc, Double)] = scoreDocs.iterator.map(loadScoreDoc)
    def jsonIterator(fields: List[Field[Doc, _]]): Iterator[(ScoreDoc, Json, Double)] = {
      scoreDocs.iterator.map { scoreDoc =>
        val json = obj(fields.map { field =>
          field.name -> jsonField(scoreDoc, field)
        }: _*)
        val score = scoreDoc.score.toDouble
        (scoreDoc, json, score)
      }
    }
    def iterator: Iterator[(V, Double)] = query.conversion match {
      case Conversion.Value(field) => scoreDocs.iterator.map { scoreDoc =>
        value(scoreDoc, field) -> scoreDoc.score.toDouble
      }
      case Conversion.Doc() => docIterator().asInstanceOf[Iterator[(V, Double)]]
      case Conversion.Converted(c) => docIterator().map {
        case (doc, score) => c(doc) -> score
      }
      case Conversion.Materialized(fields) => jsonIterator(fields).map {
        case (_, json, score) => MaterializedIndex[Doc, Model](json, model).asInstanceOf[V] -> score
      }
      case Conversion.DocAndIndexes() => jsonIterator(fields.filter(_.indexed)).map {
        case (scoreDoc, json, score) => MaterializedAndDoc[Doc, Model](json, model, loadScoreDoc(scoreDoc)._1).asInstanceOf[V] -> score
      }
      case Conversion.Json(fields) => jsonIterator(fields).map(t => t._2 -> t._3).asInstanceOf[Iterator[(V, Double)]]
      case Conversion.Distance(field, from, sort, radius) => idsAndScores.iterator.map {
        case (id, score) =>
          val state = new IndexingState
          val doc = apply(id)(transaction).sync()
          val distance = field.get(doc, field, state).map(d => Spatial.distance(from, d))
          DistanceAndDoc(doc, distance) -> score
      }
    }
    val task: Task[Iterator[(V, Double)]] = Task(iterator)
    val stream = rapid.Stream.fromIterator[(V, Double)](task)
    SearchResults(
      model = model,
      offset = query.offset,
      limit = query.limit,
      total = Some(total),
      streamWithScore = stream,
      facetResults = facetResults,
      transaction = transaction
    )
  }

  private def filter2Lucene(filter: Option[Filter[Doc]]): LuceneQuery = filter match {
    case Some(f) =>
      val fields = f.fields(model)
      def parsed(q: String, allowLeading: Boolean = false): LuceneQuery = {
        val parser = new QueryParser(f.fieldNames.head, this.index.analyzer)
        parser.setAllowLeadingWildcard(allowLeading)
        parser.setSplitOnWhitespace(true)
        parser.parse(q)
      }
      f match {
        case f: Filter.Equals[Doc, _] => exactQuery(f.field(model), f.getJson(model))
        case f: Filter.NotEquals[Doc, _] =>
          val b = new BooleanQuery.Builder
          b.add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
          b.add(exactQuery(f.field(model), f.getJson(model)), BooleanClause.Occur.MUST_NOT)
          b.build()
        case f: Filter.Regex[Doc, _] => new RegexpQuery(new Term(f.fieldName, f.expression))
        case f: Filter.In[Doc, _] =>
          val queries = f.getJson(model).map(json => exactQuery(f.field(model), json))
          val b = new BooleanQuery.Builder
          b.setMinimumNumberShouldMatch(1)
          queries.foreach { q =>
            b.add(q, BooleanClause.Occur.SHOULD)
          }
          b.build()
        case Filter.RangeLong(fieldName, from, to) => LongField.newRangeQuery(fieldName, from.getOrElse(Long.MinValue), to.getOrElse(Long.MaxValue))
        case Filter.RangeDouble(fieldName, from, to) => DoubleField.newRangeQuery(fieldName, from.getOrElse(Double.MinValue), to.getOrElse(Double.MaxValue))
        case Filter.StartsWith(_, query) if fields.head.isTokenized => parsed(s"$query*")
        case Filter.EndsWith(_, query) if fields.head.isTokenized => parsed(s"*$query", allowLeading = true)
        case Filter.Contains(_, query) if fields.head.isTokenized => parsed(s"*$query*", allowLeading = true)
        case Filter.Exact(_, query) if fields.head.isTokenized => parsed(query)
        case Filter.StartsWith(fieldName, query) => new WildcardQuery(new Term(fieldName, s"$query*"))
        case Filter.EndsWith(fieldName, query) => new WildcardQuery(new Term(fieldName, s"*$query"))
        case Filter.Contains(fieldName, query) => new WildcardQuery(new Term(fieldName, s"*$query*"))
        case Filter.Exact(fieldName, query) => new WildcardQuery(new Term(fieldName, query))
        case Filter.Distance(fieldName, from, radius) =>
          val b = new BooleanQuery.Builder
          b.add(LatLonPoint.newDistanceQuery(fieldName, from.latitude, from.longitude, radius.toMeters), BooleanClause.Occur.MUST)
          b.add(LatLonPoint.newBoxQuery(fieldName, 0.0, 0.0, 0.0, 0.0), BooleanClause.Occur.MUST_NOT)
          b.build()
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
        case Filter.DrillDownFacetFilter(fieldName, path, showOnlyThisLevel) =>
          val indexedFieldName = facetsConfig.getDimConfig(fieldName).indexFieldName
          val exactPath = if (showOnlyThisLevel) {
            path ::: List("$ROOT$")
          } else {
            path
          }
          new TermQuery(DrillDownQuery.term(indexedFieldName, fieldName, exactPath: _*))
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
      case Sort.BestMatch(direction) => direction match {
        case SortDirection.Descending => SortField.FIELD_SCORE
        case SortDirection.Ascending => new SortField(null, SortField.Type.SCORE, true)
      }
      case Sort.IndexOrder => SortField.FIELD_DOC
      case Sort.ByField(field, dir) =>
        val fieldSortName = s"${field.name}Sort"
        def st(d: DefType): SortField.Type = d match {
          case DefType.Str => SortField.Type.STRING
          case DefType.Dec => SortField.Type.DOUBLE
          case DefType.Int => SortField.Type.LONG
          case DefType.Opt(t) => st(t)
          case DefType.Arr(t) => st(t)
          case _ => throw new RuntimeException(s"Unsupported sort type for ${field.rw.definition}")
        }
        val sortType = st(field.rw.definition)
        def sf(d: DefType): SortField = d match {
          case DefType.Int | DefType.Dec => new SortedNumericSortField(fieldSortName, sortType, dir == SortDirection.Descending)
          case DefType.Str => new SortField(fieldSortName, sortType, dir == SortDirection.Descending)
          case DefType.Opt(t) => sf(t)
          case DefType.Arr(t) => sf(t)
          case d => throw new RuntimeException(s"Unsupported sort definition: $d")
        }
        sf(field.rw.definition)
      case Sort.ByDistance(field, from, _) =>
        val fieldSortName = s"${field.name}Sort"
        LatLonDocValuesField.newDistanceSort(fieldSortName, from.latitude, from.longitude)
    }
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, model)

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    aggregate(query).count

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = for {
    count <- this.count
    _ <- Task(index.indexWriter.deleteAll())
  } yield count

  override protected def doDispose(): Task[Unit] = Task {
    index.dispose()
  }
}

object LuceneStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new LuceneStore[Doc, Model](name, model, db.directory.map(_.resolve(s"$name.lucene")), storeMode)
}