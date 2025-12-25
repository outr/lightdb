package lightdb.opensearch.query

import fabric.Json
import fabric._
import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.Tokenized
import lightdb.filter.Filter
import lightdb.facet.FacetQuery
import lightdb.{Query, Sort}

/**
 * Translates LightDB query structures into OpenSearch DSL.
 *
 * Initial version is intentionally minimal; it will be expanded to full `lightdb.Query` parity.
 */
class OpenSearchSearchBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, joinScoreMode: String = "none") {
  def filterToDsl(filter: Filter[Doc]): Json = filter2Dsl(filter)

  def sortsToDsl(sorts: List[Sort]): List[Json] = sorts2Dsl(sorts)

  def exactFieldName(field: Field[Doc, _]): String = fieldNameForExact(field)

  def build[V](query: Query[Doc, Model, V]): Json = {
    val filter = query.filter.getOrElse(Filter.Multi[Doc](minShould = 0))
    val sorts = query.sort
    val base = OpenSearchDsl.searchBody(
      filter = filter2Dsl(filter),
      sorts = sorts2Dsl(sorts),
      from = query.offset,
      size = query.limit.orElse(query.pageSize),
      trackTotalHits = query.countTotal,
      minScore = query.minDocScore
    )
    if (query.facets.nonEmpty) {
      addFacetAggs(base, query.facets)
    } else {
      base
    }
  }

  private def filter2Dsl(filter: Filter[Doc]): Json = {
    filter match {
      case _: Filter.MatchNone[Doc] =>
        obj("match_none" -> obj())
      case f: Filter.ExistsChild[Doc] =>
        val relation = f.relation
        val childModel: f.ChildModel = relation.childStore.model
        val childFilter = f.childFilter(childModel)
        val childBuilder = new OpenSearchSearchBuilder[f.Child, f.ChildModel](childModel)
        val childQuery = childBuilder.filterToDsl(childFilter)
        OpenSearchDsl.hasChild(relation.childStore.name, childQuery, scoreMode = joinScoreMode)
      case f: Filter.Equals[Doc, _] =>
        val field = f.field(model)
        val json = f.getJson(model)
        if (field.name == "_id") {
          OpenSearchDsl.ids(List(jsonToIdString(json)))
        } else {
          exactQuery(field, json, boost = None)
        }
      case f: Filter.NotEquals[Doc, _] =>
        val field = f.field(model)
        val json = f.getJson(model)
        if (field.name == "_id") {
          OpenSearchDsl.boolQuery(
            must = List(OpenSearchDsl.matchAll()),
            mustNot = List(OpenSearchDsl.ids(List(jsonToIdString(json))))
          )
        } else {
          OpenSearchDsl.boolQuery(
            must = List(OpenSearchDsl.matchAll()),
            mustNot = List(exactQuery(field, json, boost = None))
          )
        }
      case f: Filter.Regex[Doc, _] =>
        val fieldName = fieldNameForPattern(f.fieldName)
        OpenSearchDsl.regexp(fieldName, f.expression)
      case f: Filter.In[Doc, _] =>
        // simplest initial mapping; later we can split by type like lucene does
        val field = f.field(model)
        if (field.name == "_id") {
          OpenSearchDsl.ids(f.getJson(model).map(jsonToIdString))
        } else {
          val fieldName = fieldNameForExact(field)
          OpenSearchDsl.terms(fieldName, f.getJson(model))
        }
      case Filter.RangeLong(fieldName, from, to) =>
        OpenSearchDsl.range(fieldName, gte = from.map(num), lte = to.map(num))
      case Filter.RangeDouble(fieldName, from, to) =>
        OpenSearchDsl.range(fieldName, gte = from.map(num), lte = to.map(num))
      case Filter.StartsWith(fieldName, q) =>
        // Prefer native prefix queries on keyword fields for speed. For tokenized fields, keep regexp on the text field.
        model.fields.find(_.name == fieldName) match {
          case Some(f) if f.isTokenized =>
            OpenSearchDsl.regexp(fieldNameForPattern(fieldName), s"${escapeRegexLiteral(q)}.*")
          case _ =>
            OpenSearchDsl.prefix(fieldNameForPattern(fieldName), q)
        }
      case Filter.EndsWith(fieldName, q) =>
        OpenSearchDsl.regexp(fieldNameForPattern(fieldName), s".*${escapeRegexLiteral(q)}")
      case Filter.Contains(fieldName, q) =>
        OpenSearchDsl.regexp(fieldNameForPattern(fieldName), s".*${escapeRegexLiteral(q)}.*")
      case Filter.Exact(fieldName, q) =>
        OpenSearchDsl.term(fieldNameForPattern(fieldName), str(q))
      case Filter.Distance(fieldName, from, radius) =>
        // OpenSearch expects a distance string like "10km"
        val centerField = s"$fieldName${lightdb.opensearch.OpenSearchTemplates.SpatialCenterSuffix}"
        val distance = OpenSearchDsl.geoDistance(centerField, from.latitude, from.longitude, s"${radius.toMeters}m")
        // Lucene indexes a dummy point (0,0) for empty geo lists and excludes it in distance filters.
        // OpenSearch rejects a 0-area bounding box, so use a tiny epsilon box around the origin.
        val eps = 1e-6
        val excludeZero = OpenSearchDsl.geoBoundingBox(centerField, eps, -eps, -eps, eps)
        OpenSearchDsl.boolQuery(must = List(distance), mustNot = List(excludeZero))
      case Filter.Multi(minShould, clauses) =>
        if (clauses.isEmpty) {
          return OpenSearchDsl.matchAll()
        }
        val must = List.newBuilder[Json]
        val filterB = List.newBuilder[Json]
        val should = List.newBuilder[Json]
        val mustNot = List.newBuilder[Json]

        clauses.foreach { c =>
          val q = filter2Dsl(c.filter)
          val boosted = c.boost match {
            case Some(b) => addBoost(q, b)
            case None => q
          }
          c.condition match {
            case lightdb.filter.Condition.Must => must += boosted
            case lightdb.filter.Condition.Filter => filterB += boosted
            case lightdb.filter.Condition.Should => should += boosted
            case lightdb.filter.Condition.MustNot => mustNot += boosted
          }
        }

        val mustList = must.result()
        val shouldList = should.result()
        val hasShould = clauses.exists(c => c.condition == lightdb.filter.Condition.Should || c.condition == lightdb.filter.Condition.Filter)
        val minimum = if (hasShould) Some(minShould).filter(_ > 0) else None

        // Lucene adds MatchAll MUST when there are only SHOULDs and minShould==0.
        val mustWithMatchAll = if (minimum.isEmpty && mustList.isEmpty && shouldList.nonEmpty) {
          OpenSearchDsl.matchAll() :: mustList
        } else {
          mustList
        }

        OpenSearchDsl.boolQuery(
          must = mustWithMatchAll,
          filter = filterB.result(),
          should = shouldList,
          mustNot = mustNot.result(),
          minimumShouldMatch = minimum
        )
      case Filter.DrillDownFacetFilter(_, _, _) =>
        val f = filter.asInstanceOf[Filter.DrillDownFacetFilter[Doc]]
        drillDownFacetFilter(f)
    }
  }

  private val RootMarker: String = "$ROOT$"

  private def drillDownFacetFilter(f: Filter.DrillDownFacetFilter[Doc]): Json = {
    val tokenField = s"${f.fieldName}__facet"
    if (f.path.isEmpty && !f.showOnlyThisLevel) {
      // Approximate Lucene "drillDown()" as "has this facet dimension at any level".
      OpenSearchDsl.exists(tokenField)
    } else {
      val token = if (f.showOnlyThisLevel) {
        if (f.path.isEmpty) RootMarker else s"${f.path.mkString("/")}/$RootMarker"
      } else {
        f.path.mkString("/")
      }
      OpenSearchDsl.term(tokenField, str(token))
    }
  }

  private def addFacetAggs(body: Json, facets: List[FacetQuery[Doc]]): Json = {
    val aggs = obj(facets.map(fq => aggName(fq) -> facetAgg(fq)): _*)
    body match {
      case o: Obj => obj((o.value.toSeq :+ ("aggregations" -> aggs)): _*)
      case _ => body
    }
  }

  private def jsonToIdString(json: Json): String = json match {
    case Str(s, _) => s
    case other => other.toString
  }

  private def aggName(fq: FacetQuery[Doc]): String = s"facet_${fq.field.name}"

  private def facetAgg(fq: FacetQuery[Doc]): Json = {
    val tokenField = s"${fq.field.name}__facet"
    // NOTE: OpenSearch `terms.include` uses Lucene regexp syntax (not Java regex).
    // Rather than rely on tricky escaping/anchoring, we overfetch and post-filter in Scala.
    val size = fq.childrenLimit.getOrElse(10_000)
    obj("terms" -> obj(
      "field" -> str(tokenField),
      "size" -> num(size),
      "order" -> obj("_count" -> str("desc"))
    ))
  }

  // Filtering is done in OpenSearchTransaction when parsing aggregation buckets.

  private def sorts2Dsl(sorts: List[Sort]): List[Json] = {
    sorts.map {
      case Sort.IndexOrder =>
        obj("_id" -> obj("order" -> str("asc")))
      case Sort.BestMatch(direction) =>
        val order = if (direction == lightdb.SortDirection.Descending) "desc" else "asc"
        obj("_score" -> obj("order" -> str(order)))
      case s: Sort.ByField[_, _] =>
        val order = if (s.direction == lightdb.SortDirection.Descending) "desc" else "asc"
        val fieldName = sortFieldName(s.field.asInstanceOf[Field[Doc, _]])
        obj(fieldName -> obj("order" -> str(order)))
      case s: Sort.ByDistance[_, _] =>
        val order = if (s.direction == lightdb.SortDirection.Descending) "desc" else "asc"
        obj("_geo_distance" -> obj(
          s"${s.field.name}${lightdb.opensearch.OpenSearchTemplates.SpatialCenterSuffix}" ->
            obj("lat" -> num(s.from.latitude), "lon" -> num(s.from.longitude)),
          "order" -> str(order),
          "unit" -> str("m")
        ))
    }
  }

  private def exactQuery(field: Field[Doc, _], json: Json, boost: Option[Double]): Json = json match {
    case Str(s, _) if field.isInstanceOf[Tokenized[_]] =>
      val tokens = s.split("\\s+").toList.filter(_.nonEmpty).map(_.toLowerCase)
      val must = if (tokens.nonEmpty) tokens.map(t => OpenSearchDsl.term(field.name, str(t))) else List(OpenSearchDsl.matchAll())
      addBoost(OpenSearchDsl.boolQuery(must = must), boost.getOrElse(1.0))
    case Str(s, _) =>
      OpenSearchDsl.term(fieldNameForExact(field), str(s), boost)
    case Bool(b, _) =>
      OpenSearchDsl.term(field.name, bool(b), boost)
    case NumInt(l, _) =>
      OpenSearchDsl.term(field.name, num(l), boost)
    case NumDec(bd, _) =>
      OpenSearchDsl.term(field.name, num(bd), boost)
    case Arr(values, _) =>
      // Lucene treats array equality as MUST of each element.
      val must = values.toList.map(v => exactQuery(field, v, boost = None))
      addBoost(OpenSearchDsl.boolQuery(must = must), boost.getOrElse(1.0))
    case Null =>
      // For Option fields, OpenSearch does not index nulls. Treat Equals(null) as "missing".
      addBoost(OpenSearchDsl.boolQuery(mustNot = List(OpenSearchDsl.exists(field.name))), boost.getOrElse(1.0))
    case j =>
      throw new RuntimeException(s"Unsupported equality check: $j (${field.rw.definition})")
  }

  private def addBoost(query: Json, boost: Double): Json = {
    // Prefer native boosting on bool queries (preserves internal scoring like Lucene),
    // otherwise fall back to function_score weight.
    query match {
      case o: Obj if o.value.size == 1 && o.value.contains("bool") =>
        o.value("bool") match {
          case b: Obj =>
            obj("bool" -> obj((b.value.toSeq :+ ("boost" -> num(boost))): _*))
          case _ =>
            functionScore(query, boost)
        }
      case _ =>
        functionScore(query, boost)
    }
  }

  private def functionScore(query: Json, boost: Double): Json =
    obj("function_score" -> obj(
      "query" -> query,
      "functions" -> arr(obj("weight" -> num(boost))),
      "score_mode" -> str("multiply"),
      "boost_mode" -> str("multiply")
    ))

  private def sortFieldName(field: Field[Doc, _]): String = field.rw.definition match {
    case DefType.Str | DefType.Enum(_, _) => s"${field.name}.keyword"
    case DefType.Opt(d) => sortFieldNameFromDef(field.name, d)
    case DefType.Arr(d) => sortFieldNameFromDef(field.name, d)
    case _ => field.name
  }

  private def sortFieldNameFromDef(fieldName: String, d: DefType): String = d match {
    case DefType.Str | DefType.Enum(_, _) => s"$fieldName.keyword"
    case DefType.Opt(inner) => sortFieldNameFromDef(fieldName, inner)
    case DefType.Arr(inner) => sortFieldNameFromDef(fieldName, inner)
    case _ => fieldName
  }

  private val regexChars = ".?+*|{}[]()\"\\#".toSet
  private def escapeRegexLiteral(s: String): String =
    s.flatMap(c => if (regexChars.contains(c)) s"\\$c" else c.toString)

  private def fieldNameForExact(field: Field[Doc, _]): String = field.rw.definition match {
    case DefType.Str | DefType.Enum(_, _) if !field.isInstanceOf[Tokenized[_]] => s"${field.name}.keyword"
    case DefType.Opt(d) => fieldNameForExactFromDef(field.name, d, tokenized = field.isInstanceOf[Tokenized[_]])
    case DefType.Arr(d) => fieldNameForExactFromDef(field.name, d, tokenized = field.isInstanceOf[Tokenized[_]])
    case _ => field.name
  }

  private def fieldNameForExactFromDef(fieldName: String, d: DefType, tokenized: Boolean): String = d match {
    case DefType.Str | DefType.Enum(_, _) if !tokenized => s"$fieldName.keyword"
    case DefType.Opt(inner) => fieldNameForExactFromDef(fieldName, inner, tokenized)
    case DefType.Arr(inner) => fieldNameForExactFromDef(fieldName, inner, tokenized)
    case _ => fieldName
  }

  private def fieldNameForPattern(fieldName: String): String =
    model.fields.find(_.name == fieldName) match {
      case Some(f) if f.isTokenized => fieldName
      case Some(f) if f.rw.definition == DefType.Str || f.rw.definition.isInstanceOf[DefType.Enum] => s"$fieldName.keyword"
      case Some(f) => fieldNameForExact(f.asInstanceOf[Field[Doc, _]])
      case None => fieldName
    }
}


