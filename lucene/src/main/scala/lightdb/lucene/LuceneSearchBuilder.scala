package lightdb.lucene

import fabric.define.DefType
import fabric.rw.Asable
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Str, obj}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.field.Field.Tokenized
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{Condition, Filter}
import lightdb.id.Id
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Spatial}
import lightdb.store.{Conversion, StoreMode}
import lightdb.{Query, SearchResults, Sort, SortDirection}
import org.apache.lucene.document._
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts
import org.apache.lucene.facet.{DrillDownQuery, FacetsCollector, FacetsCollectorManager}
import org.apache.lucene.index.{StoredFields, Term}
import org.apache.lucene.search.grouping.{FirstPassGroupingCollector, SearchGroup, TermGroupSelector, TopGroups, TopGroupsCollector}
import org.apache.lucene.search.{BooleanClause, BooleanQuery, BoostQuery, MatchAllDocsQuery, MultiCollectorManager, RegexpQuery, ScoreDoc, SortField, SortedNumericSortField, TermInSetQuery, TermQuery, TopFieldCollectorManager, TopFieldDocs, TotalHitCountCollectorManager, Query => LuceneQuery, Sort => LuceneSort}
import org.apache.lucene.util.BytesRef
import org.apache.lucene.util.automaton.RegExp
import rapid.Task

import scala.jdk.CollectionConverters._

class LuceneSearchBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: LuceneStore[Doc, Model],
                                                                             model: Model,
                                                                             tx: LuceneTransaction[Doc, Model]) {
  def apply[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task {
    val indexSearcher = tx.state.indexSearcher
    val q: LuceneQuery = filter2Lucene(query.filter).rewrite(indexSearcher)
    val s: LuceneSort = sort(query.sort)
    val limit = query.limit.orElse(query.pageSize).getOrElse(100_000_000)   // TODO: Evaluate a configurable upper limit
    if (limit <= 0) throw new RuntimeException(s"Limit must be a positive value, but set to $limit")
    val max = limit + query.offset

    val collectors = List(
      Some(new TopFieldCollectorManager(s, max, max)),
      if (query.countTotal) Some(new TotalHitCountCollectorManager(indexSearcher.getSlices)) else None,
      if (query.facets.nonEmpty) Some(new FacetsCollectorManager(query.scoreDocs)) else None
    ).flatten
    val manager = new MultiCollectorManager(collectors: _*)
    val resultCollectors = indexSearcher.search(q, manager)
    val topFieldDocs = resultCollectors.collectFirst {
      case tfd: TopFieldDocs => tfd
    }.getOrElse(throw new RuntimeException("TopFieldDocs not found!"))
    val actualCount = resultCollectors.collectFirst {
      case i: java.lang.Integer => i.intValue()
    }
    val facetsCollector = resultCollectors.collectFirst {
      case fc: FacetsCollector => fc
    }
    val facetResults = facetsCollector.map { fc =>
      val facets = new FastTaxonomyFacetCounts(tx.state.taxonomyReader, store.facetsConfig, fc)
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
    }.getOrElse(Map.empty)

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
    val total: Int = actualCount.getOrElse(topFieldDocs.totalHits.value.toInt)
    val storedFields: StoredFields = indexSearcher.storedFields()
    val iterator: Iterator[(V, Double)] = materializeScoreDocs(query, scoreDocs, storedFields)
    val task: Task[Iterator[(V, Double)]] = Task(iterator)
    val stream = rapid.Stream.fromIterator[(V, Double)](task)
    SearchResults(
      model = model,
      offset = query.offset,
      limit = query.limit,
      total = Some(total),
      streamWithScore = stream,
      facetResults = facetResults,
      transaction = tx
    )
  }

  private def materializeScoreDocs[V](query: Query[Doc, Model, V],
                                      scoreDocs: List[ScoreDoc],
                                      storedFields: StoredFields): Iterator[(V, Double)] = {
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
    def loadScoreDoc(scoreDoc: ScoreDoc): (Doc, Double) = store.storeMode match {
      case StoreMode.All() =>
        model match {
          case c: JsonConversion[Doc] =>
            val o = obj(store.fields.map(f => f.name -> jsonField(scoreDoc, f)): _*)
            c.convertFromJson(o) -> scoreDoc.score.toDouble
          case _ =>
            val map = store.fields.map { field =>
              field.name -> value(scoreDoc, field)
            }.toMap
            model.map2Doc(map) -> scoreDoc.score.toDouble
        }
      case StoreMode.Indexes(_) =>
        val docId = scoreDoc.doc
        val id = Id[Doc](storedFields.document(docId).get("_id"))
        val score = scoreDoc.score.toDouble
        tx.parent.get(id).sync() -> score
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
    query.conversion match {
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
      case Conversion.DocAndIndexes() => jsonIterator(store.fields.filter(_.indexed)).map {
        case (scoreDoc, json, score) => MaterializedAndDoc[Doc, Model](json, model, loadScoreDoc(scoreDoc)._1).asInstanceOf[V] -> score
      }
      case Conversion.Json(fields) => jsonIterator(fields).map(t => t._2 -> t._3).asInstanceOf[Iterator[(V, Double)]]
      case Conversion.Distance(field, from, sort, radius) => idsAndScores.iterator.map {
        case (id, score) =>
          val state = new IndexingState
          val doc = tx.parent.getOrElse(tx)(id).sync()
          val distance = field.get(doc, field, state).map(d => Spatial.distance(from, d))
          DistanceAndDoc(doc, distance) -> score
      }
    }
  }

  def grouped[G, V](query: Query[Doc, Model, V],
                    groupField: Field[Doc, G],
                    docsPerGroup: Int,
                    groupOffset: Int,
                    groupLimit: Int,
                    groupSortList: List[Sort],
                    withinGroupSortList: List[Sort],
                    includeScores: Boolean,
                    includeTotalGroupCount: Boolean): Task[LuceneGroupedSearchResults[Doc, Model, G, V]] = Task {
    if (groupLimit <= 0) throw new RuntimeException(s"Group limit must be positive but was $groupLimit")
    if (docsPerGroup <= 0) throw new RuntimeException(s"Docs per group must be positive but was $docsPerGroup")

    val indexSearcher = tx.state.indexSearcher
    val luceneQuery = filter2Lucene(query.filter).rewrite(indexSearcher)
    val groupSort = sort(groupSortList)
    val withinGroupSort = sort(if (withinGroupSortList.nonEmpty) withinGroupSortList else groupSortList)
    val topNGroups = groupOffset + groupLimit
    val groupFieldName = s"${groupField.name}Sort"
    val groupSelector = new TermGroupSelector(groupFieldName)

    val firstPass = new FirstPassGroupingCollector[BytesRef](groupSelector, groupSort, topNGroups)
    indexSearcher.search(luceneQuery, firstPass)
    val rawTopGroups: List[SearchGroup[BytesRef]] = Option(firstPass.getTopGroups(groupOffset))
      .map(_.asScala.toList)
      .getOrElse(Nil)

    if (rawTopGroups.isEmpty) {
      LuceneGroupedSearchResults(
        model = model,
        offset = groupOffset,
        limit = Some(groupLimit),
        totalGroups = if (includeTotalGroupCount) Some(0) else None,
        groups = Nil,
        transaction = tx
      )
    } else {
      val secondPass = new TopGroupsCollector[BytesRef](
        groupSelector,
        rawTopGroups.asJava,
        groupSort,
        withinGroupSort,
        docsPerGroup,
        includeScores
      )
      indexSearcher.search(luceneQuery, secondPass)
      val topGroupsResult: Option[TopGroups[BytesRef]] = Option(secondPass.getTopGroups(groupOffset))
      val storedFields: StoredFields = indexSearcher.storedFields()
      val groupedResults = topGroupsResult
        .map(_.groups.toList)
        .getOrElse(Nil)
        .map { gd =>
          val docs = materializeScoreDocs(query, gd.scoreDocs.toList, storedFields).toList
          LuceneGroupedResult(
            group = decodeGroupValue(gd.groupValue, groupField),
            results = docs
          )
        }
      val totalGroups = if (includeTotalGroupCount) {
        topGroupsResult.flatMap(tg => Option(tg.totalGroupCount).filter(_ >= 0).map(_.intValue()))
          .orElse(Some(rawTopGroups.size))
      } else {
        None
      }
      LuceneGroupedSearchResults(
        model = model,
        offset = groupOffset,
        limit = Some(groupLimit),
        totalGroups = totalGroups,
        groups = groupedResults,
        transaction = tx
      )
    }
  }

  private def decodeGroupValue[G](groupValue: BytesRef, field: Field[Doc, G]): G = {
    if (groupValue == null) {
      field.rw.definition match {
        case DefType.Opt(_) => None.asInstanceOf[G]
        case _ => throw new RuntimeException(s"Missing group value for grouping field '${field.name}'")
      }
    } else {
      val asString = groupValue.utf8ToString
      Field.string2Json(field.name, asString, field.rw.definition).as[G](field.rw)
    }
  }

  private def sort(sort: List[Sort]): LuceneSort = {
    val sortFields = sort match {
      case Nil => List(SortField.FIELD_SCORE)
      case _ => sort.map(sort2SortField)
    }
    new LuceneSort(sortFields: _*)
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

  def filter2Lucene(filter: Option[Filter[Doc]]): LuceneQuery = filter match {
    case Some(f) =>
      f match {
        case f: Filter.Equals[Doc, _] => exactQuery(f.field(model), f.getJson(model))
        case f: Filter.NotEquals[Doc, _] =>
          val b = new BooleanQuery.Builder
          b.add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
          b.add(exactQuery(f.field(model), f.getJson(model)), BooleanClause.Occur.MUST_NOT)
          b.build()
        case f: Filter.Regex[Doc, _] => new RegexpQuery(new Term(f.fieldName, f.expression), RegExp.ALL, 10_000_000)
        case f: Filter.In[Doc, _] =>
          val values = f.getJson(model)
          val fieldName = f.field(model).name
          val bytesRefs = values.collect {
            case Str(s, _) => new BytesRef(s)
            case NumInt(i, _) => new BytesRef(i.toString)
            case NumDec(d, _) => new BytesRef(d.toString)
            case Bool(b, _) => new BytesRef(if (b) "1" else "0")
          }
          if (bytesRefs.size == values.size) {
            // All simple string IDs → use TermInSetQuery to avoid overflow
            new TermInSetQuery(fieldName, bytesRefs.asJava)
          } else {
            // fallback to BooleanQuery for mixed types
            val queries = values.map(json => exactQuery(f.field(model), json))
            val b = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1)
            queries.foreach(q => b.add(q, BooleanClause.Occur.SHOULD))
            b.build()
          }
        case Filter.RangeLong(fieldName, from, to) => LongField.newRangeQuery(fieldName, from.getOrElse(Long.MinValue), to.getOrElse(Long.MaxValue))
        case Filter.RangeDouble(fieldName, from, to) => DoubleField.newRangeQuery(fieldName, from.getOrElse(Double.MinValue), to.getOrElse(Double.MaxValue))
        case Filter.StartsWith(fieldName, query) => new RegexpQuery(new Term(fieldName, s"${LuceneStore.escapeRegexLiteral(query)}.*"))
        case Filter.EndsWith(fieldName, query) => new RegexpQuery(new Term(fieldName, s".*${LuceneStore.escapeRegexLiteral(query)}"))
        case Filter.Contains(fieldName, query) => new RegexpQuery(new Term(fieldName, s".*${LuceneStore.escapeRegexLiteral(query)}.*"))
        case Filter.Exact(fieldName, query) => new TermQuery(new Term(fieldName, query))
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
          val indexedFieldName = store.facetsConfig.getDimConfig(fieldName).indexFieldName
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
      s.split("\\s+").foreach { token =>
        val normalized = token.toLowerCase
        b.add(new TermQuery(new Term(field.name, normalized)), BooleanClause.Occur.MUST)
      }
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
}
