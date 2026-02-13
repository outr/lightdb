package lightdb.opensearch.query

import fabric.Json
import fabric.*
import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.Tokenized
import lightdb.filter.Filter
import lightdb.facet.FacetQuery
import lightdb.store.Conversion
import lightdb.{Query, Sort}

/**
 * Translates LightDB query structures into OpenSearch DSL.
 *
 * Initial version is intentionally minimal; it will be expanded to full `lightdb.Query` parity.
 */
class OpenSearchSearchBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                                joinScoreMode: String = "none",
                                                                                keywordNormalize: Boolean = false,
                                                                                allowScriptSorts: Boolean = false,
                                                                                facetAggMaxBuckets: Int = 65_536,
                                                                                facetChildCountMode: String = "cardinality",
                                                                                facetChildCountPrecisionThreshold: Option[Int] = None,
                                                                                facetChildCountPageSize: Int = 1000) {
  private val InternalIdFieldName: String = lightdb.opensearch.OpenSearchTemplates.InternalIdField

  private def rewriteReservedIdFieldName(fieldName: String): String =
    if fieldName == "_id" then InternalIdFieldName else fieldName

  private def normalizeKeyword(s: String): String =
    if keywordNormalize then s.trim.toLowerCase else s

  private def shouldNormalizeKeyword(field: Field[Doc, _], fieldName: String): Boolean = {
    keywordNormalize && fieldName.endsWith(".keyword")
  }

  private def hasModelField(fieldName: String): Boolean =
    model.fields.exists(_.name == fieldName)

  def filterToDsl(filter: Filter[Doc]): Json = filter2Dsl(filter)

  def sortsToDsl(sorts: List[Sort]): List[Json] = sorts2Dsl(sorts)

  def exactFieldName(field: Field[Doc, _]): String = fieldNameForExact(field)

  def build[V](query: Query[Doc, Model, V]): Json = {
    val filter = query.filter.getOrElse(Filter.Multi[Doc](minShould = 0))
    val sorts = query.sort
    val hasScoreSort = sorts.exists {
      case Sort.BestMatch(_) => true
      case _ => false
    }
    // OpenSearch returns `_score=null` when sorting by non-score fields unless `track_scores=true`.
    // Only enable this when the caller explicitly requested scoring but isn't already sorting by `_score`.
    val trackScores = query.scoreDocs && !hasScoreSort
    val base = OpenSearchDsl.searchBody(
      filter = filter2Dsl(filter),
      sorts = sorts2Dsl(sorts),
      from = query.offset,
      size = query.limit.orElse(query.pageSize),
      trackTotalHits = query.countTotal,
      trackScores = trackScores,
      minScore = query.minDocScore
    )
    // PERF:
    // For field-only conversions, request a filtered `_source` to avoid returning (and parsing) giant documents.
    // This materially reduces memory/GC pressure for large scans such as:
    // - Query.value(...)
    // - Query.json(fields=...)
    // - Query.materialized(fields=...)
    val sourceFiltered: Json = {
      val includes: Option[List[String]] = query.conversion match {
        case Conversion.Value(field) =>
          // `_id` is not in `_source` (it's a hit metadata field), so don't request it.
          if field.name == "_id" then Some(Nil) else Some(List(field.name))
        case Conversion.Json(fields) =>
          Some(fields.map(_.name).filterNot(_ == "_id"))
        case Conversion.Materialized(fields) =>
          Some(fields.map(_.name).filterNot(_ == "_id"))
        case _ =>
          None
      }

      includes match {
        case None =>
          base
        case Some(Nil) =>
          // Only safe when the conversion doesn't need `_source` (ex: Value(_id)).
          // Otherwise we'd produce Nulls for all requested fields.
          query.conversion match {
            case Conversion.Value(field) if field.name == "_id" =>
              base match {
                case o: Obj => obj((o.value.toSeq :+ ("_source" -> fabric.bool(false))): _*)
                case other => other
              }
            case _ =>
              base
          }
        case Some(fieldNames) =>
          base match {
            case o: Obj =>
              obj((o.value.toSeq :+ ("_source" -> obj("includes" -> arr(fieldNames.map(str): _*)))): _*)
            case other =>
              other
          }
      }
    }
    if query.facets.nonEmpty then {
      addFacetAggs(sourceFiltered, query.facets)
    } else {
      sourceFiltered
    }
  }

  private def filter2Dsl(filter: Filter[Doc]): Json = {
    filter match {
      case _: Filter.MatchNone[Doc] =>
        obj("match_none" -> obj())
      case f: Filter.Nested[Doc] =>
        if !model.nestedPaths.contains(f.path) then {
          throw new IllegalArgumentException(
            s"Nested path '${f.path}' is not declared on model ${model.modelName}. " +
              s"Declare it via nestedPath(\"${f.path}\") in the model."
          )
        }
        val nestedFilter = rewriteFilterFieldsForNestedPath(f.path, f.filter)
        val nestedQuery = filter2Dsl(nestedFilter)
        OpenSearchDsl.nested(path = f.path, query = nestedQuery)
      case f: Filter.ChildConstraints[Doc @unchecked] =>
        val relation = f.relation
        val childModel: f.ChildModel = relation.childStore.model
        val childBuilder = new OpenSearchSearchBuilder[f.Child, f.ChildModel](
          childModel,
          keywordNormalize = keywordNormalize,
          facetAggMaxBuckets = facetAggMaxBuckets,
          facetChildCountMode = facetChildCountMode,
          facetChildCountPrecisionThreshold = facetChildCountPrecisionThreshold,
          facetChildCountPageSize = facetChildCountPageSize
        )
        f.semantics match {
          case Filter.ChildSemantics.SameChildAll =>
            val childMust = f.builds.map(b => childBuilder.filterToDsl(b(childModel)))
            val childQuery = OpenSearchDsl.boolQuery(must = if childMust.isEmpty then List(OpenSearchDsl.matchAll()) else childMust)
            OpenSearchDsl.hasChild(relation.childStore.name, childQuery, scoreMode = joinScoreMode)
          case Filter.ChildSemantics.CollectiveAll =>
            // AND of has_child clauses (each constraint can match a different child)
            val must = f.builds.map { b =>
              val cq = childBuilder.filterToDsl(b(childModel))
              OpenSearchDsl.hasChild(relation.childStore.name, cq, scoreMode = joinScoreMode)
            }
            if must.isEmpty then OpenSearchDsl.matchAll() else OpenSearchDsl.boolQuery(must = must)
        }
      case f: Filter.ExistsChild[Doc] =>
        val relation = f.relation
        val childModel: f.ChildModel = relation.childStore.model
        val childFilter = f.childFilter(childModel)
        val childBuilder = new OpenSearchSearchBuilder[f.Child, f.ChildModel](
          childModel,
          joinScoreMode = "none",
          keywordNormalize = keywordNormalize,
          facetAggMaxBuckets = facetAggMaxBuckets,
          facetChildCountMode = facetChildCountMode,
          facetChildCountPrecisionThreshold = facetChildCountPrecisionThreshold,
          facetChildCountPageSize = facetChildCountPageSize
        )
        val childQuery = childBuilder.filterToDsl(childFilter)
        OpenSearchDsl.hasChild(relation.childStore.name, childQuery, scoreMode = joinScoreMode)
      case f: Filter.Equals[Doc, _] =>
        val field = f.field(model)
        val json = f.getJson(model)
        if field.name == "_id" then {
          OpenSearchDsl.ids(List(jsonToIdString(json)))
        } else {
          exactQuery(field, json, boost = None)
        }
      case f: Filter.NotEquals[Doc, _] =>
        model.fields.find(_.name == f.fieldName) match {
          case Some(field) =>
            val json = f.getJson(model)
            if field.name == "_id" then {
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
          case None =>
            // Fallback for unknown fields (e.g., nested subfields like "attrs.key" not registered in model)
            val json = f.value match {
              case s: String => str(s)
              case b: Boolean => bool(b)
              case i: Int => num(i)
              case l: Long => num(l)
              case d: Double => num(d)
              case bd: BigDecimal => num(bd)
              case other => throw new IllegalArgumentException(
                s"Unsupported NotEquals value type for field '${f.fieldName}': ${other.getClass.getName}"
              )
            }
            val base = rewriteReservedIdFieldName(f.fieldName)
            val keyword = s"$base.keyword"
            OpenSearchDsl.boolQuery(
              must = List(OpenSearchDsl.matchAll()),
              mustNot = List(OpenSearchDsl.boolQuery(
                should = List(
                  OpenSearchDsl.term(keyword, json, boost = None),
                  OpenSearchDsl.term(base, json, boost = None)
                ),
                minimumShouldMatch = Some(1)
              ))
            )
        }
      case f: Filter.Regex[Doc, _] =>
        val fieldName = fieldNameForPattern(f.fieldName)
        OpenSearchDsl.regexp(fieldName, f.expression)
      case f: Filter.In[Doc, _] =>
        // simplest initial mapping; later we can split by type like lucene does
        model.fields.find(_.name == f.fieldName) match {
          case Some(field) =>
            if field.name == "_id" then {
              OpenSearchDsl.ids(f.getJson(model).map(jsonToIdString))
            } else {
              val base = rewriteReservedIdFieldName(field.name)
              val keyword = s"$base.keyword"
              val values = if keywordNormalize then {
                f.getJson(model).map {
                  case Str(s, _) => str(normalizeKeyword(s))
                  case other => other
                }
              } else {
                f.getJson(model)
              }

              // Compatibility: some indices map non-tokenized strings/enums as `keyword` directly (no `.keyword` subfield),
              // while others use `text` with a `.keyword` multifield. Use OR so both mapping styles match.
              OpenSearchDsl.boolQuery(
                should = List(
                  OpenSearchDsl.terms(keyword, values),
                  OpenSearchDsl.terms(base, values)
                ),
                minimumShouldMatch = Some(1)
              )
            }
          case None =>
            // Fallback for unknown fields (e.g., nested subfields like "attrs.key" not registered in model)
            val base = rewriteReservedIdFieldName(f.fieldName)
            val keyword = s"$base.keyword"
            val values = f.values.map {
              case s: String => str(if keywordNormalize then normalizeKeyword(s) else s)
              case b: Boolean => bool(b)
              case i: Int => num(i)
              case l: Long => num(l)
              case d: Double => num(d)
              case bd: BigDecimal => num(bd)
              case other => throw new IllegalArgumentException(
                s"Unsupported In value type for field '${f.fieldName}': ${other.getClass.getName}"
              )
            }

            // Compatibility: some indices map non-tokenized strings/enums as `keyword` directly (no `.keyword` subfield),
            // while others use `text` with a `.keyword` multifield. Use OR so both mapping styles match.
            OpenSearchDsl.boolQuery(
              should = List(
                OpenSearchDsl.terms(keyword, values),
                OpenSearchDsl.terms(base, values)
              ),
              minimumShouldMatch = Some(1)
            )
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
          case None =>
            val base = rewriteReservedIdFieldName(fieldName)
            val keyword = s"$base.keyword"
            val v = if keywordNormalize then normalizeKeyword(q) else q
            OpenSearchDsl.boolQuery(
              should = List(
                OpenSearchDsl.prefix(keyword, v),
                OpenSearchDsl.prefix(base, v)
              ),
              minimumShouldMatch = Some(1)
            )
          case _ =>
            val fn = fieldNameForPattern(fieldName)
            // fn is a keyword field for non-tokenized strings.
            val v = if keywordNormalize then normalizeKeyword(q) else q
            OpenSearchDsl.prefix(fn, v)
        }
      case Filter.EndsWith(fieldName, q) =>
        val v = if keywordNormalize then normalizeKeyword(q) else q
        if hasModelField(fieldName) then {
          val fn = fieldNameForPattern(fieldName)
          OpenSearchDsl.regexp(fn, s".*${escapeRegexLiteral(v)}")
        } else {
          val base = rewriteReservedIdFieldName(fieldName)
          val keyword = s"$base.keyword"
          OpenSearchDsl.boolQuery(
            should = List(
              OpenSearchDsl.regexp(keyword, s".*${escapeRegexLiteral(v)}"),
              OpenSearchDsl.regexp(base, s".*${escapeRegexLiteral(v)}")
            ),
            minimumShouldMatch = Some(1)
          )
        }
      case Filter.Contains(fieldName, q) =>
        val v = if keywordNormalize then normalizeKeyword(q) else q
        if hasModelField(fieldName) then {
          val fn = fieldNameForPattern(fieldName)
          OpenSearchDsl.regexp(fn, s".*${escapeRegexLiteral(v)}.*")
        } else {
          val base = rewriteReservedIdFieldName(fieldName)
          val keyword = s"$base.keyword"
          OpenSearchDsl.boolQuery(
            should = List(
              OpenSearchDsl.regexp(keyword, s".*${escapeRegexLiteral(v)}.*"),
              OpenSearchDsl.regexp(base, s".*${escapeRegexLiteral(v)}.*")
            ),
            minimumShouldMatch = Some(1)
          )
        }
      case Filter.Exact(fieldName, q) =>
        val v = if keywordNormalize then normalizeKeyword(q) else q
        if hasModelField(fieldName) then {
          val fn = fieldNameForPattern(fieldName)
          OpenSearchDsl.term(fn, str(v))
        } else {
          val base = rewriteReservedIdFieldName(fieldName)
          val keyword = s"$base.keyword"
          OpenSearchDsl.boolQuery(
            should = List(
              OpenSearchDsl.term(keyword, str(v)),
              OpenSearchDsl.term(base, str(v))
            ),
            minimumShouldMatch = Some(1)
          )
        }
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
        if clauses.isEmpty then {
          OpenSearchDsl.matchAll()
        } else {
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
          val minimum = if shouldList.nonEmpty then Some(minShould).filter(_ > 0) else None
          val filterList = filterB.result()

          // Lucene adds MatchAll MUST when there are only SHOULDs and minShould==0.
          val mustWithMatchAll = if minimum.isEmpty && mustList.isEmpty && shouldList.nonEmpty then {
            OpenSearchDsl.matchAll() :: mustList
          } else if mustList.isEmpty && shouldList.isEmpty && filterList.nonEmpty then {
            // Lucene-style constant scoring: filters without any scoring clauses should still produce a constant score.
            OpenSearchDsl.matchAll() :: mustList
          } else {
            mustList
          }

          OpenSearchDsl.boolQuery(
            must = mustWithMatchAll,
            filter = filterList,
            should = shouldList,
            mustNot = mustNot.result(),
            minimumShouldMatch = minimum
          )
        }
      case Filter.DrillDownFacetFilter(_, _, _) =>
        val f = filter.asInstanceOf[Filter.DrillDownFacetFilter[Doc]]
        drillDownFacetFilter(f)
    }
  }

  private def rewriteFilterFieldsForNestedPath(path: String, filter: Filter[Doc]): Filter[Doc] = {
    val prefix = s"$path."

    def qualify(fieldName: String): String =
      if fieldName.startsWith(prefix) then fieldName else s"$prefix$fieldName"

    filter match {
      case f: Filter.Equals[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.NotEquals[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.Regex[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.In[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.RangeLong[Doc] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.RangeDouble[Doc] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.StartsWith[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.EndsWith[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.Contains[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.Exact[Doc, _] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.Distance[Doc] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.DrillDownFacetFilter[Doc] =>
        f.copy(fieldName = qualify(f.fieldName))
      case f: Filter.Multi[Doc] =>
        f.copy(filters = f.filters.map(clause => clause.copy(filter = rewriteFilterFieldsForNestedPath(path, clause.filter))))
      case f: Filter.Nested[Doc] =>
        // Preserve nested boundaries; inner nested path can be absolute or relative to this path.
        val nestedPath = if f.path.startsWith(prefix) then f.path else s"$prefix${f.path}"
        f.copy(path = nestedPath, filter = rewriteFilterFieldsForNestedPath(nestedPath, f.filter))
      case other =>
        other
    }
  }

  private val RootMarker: String = "$ROOT$"

  private def drillDownFacetFilter(f: Filter.DrillDownFacetFilter[Doc]): Json = {
    val tokenField = s"${f.fieldName}__facet"
    if f.path.isEmpty && !f.showOnlyThisLevel then {
      // Approximate Lucene "drillDown()" as "has this facet dimension at any level".
      OpenSearchDsl.exists(tokenField)
    } else {
      val token = if f.showOnlyThisLevel then {
        if f.path.isEmpty then RootMarker else s"${f.path.mkString("/")}/$RootMarker"
      } else {
        f.path.mkString("/")
      }
      OpenSearchDsl.term(tokenField, str(token))
    }
  }

  private def addFacetAggs(body: Json, facets: List[FacetQuery[Doc]]): Json = {
    val aggs = obj(facets.flatMap { fq =>
      val valuesAgg = aggName(fq) -> facetAggValues(fq)
      val countAgg = aggCountName(fq) -> facetAggChildCount(fq)
      List(valuesAgg, countAgg)
    }: _*)
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

  private def aggCountName(fq: FacetQuery[Doc]): String = s"facet_count_${fq.field.name}"

  private def facetAggValues(fq: FacetQuery[Doc]): Json = {
    val tokenField = s"${fq.field.name}__facet"
    // Values list (top-N). Exact childCount is computed separately via composite agg paging.
    val size = fq.childrenLimit match {
      case Some(l) => l
      case None => facetAggMaxBuckets
    }
    obj("terms" -> obj(
      "field" -> str(tokenField),
      "size" -> num(size),
      "order" -> obj("_count" -> str("desc"))
    ))
  }

  private def facetAggChildCount(fq: FacetQuery[Doc]): Json = {
    val tokenField = s"${fq.field.name}__facet"
    val mode = facetChildCountMode.trim.toLowerCase
    // Hierarchical facets cannot use a plain cardinality aggregation because the indexed tokens include
    // prefixes (year, year/month, year/month/day, ...). LightDB's `childCount` for hierarchical facets
    // must reflect distinct children at the requested path, which requires composite token enumeration.
    val useComposite = mode == "composite" || fq.path.nonEmpty || fq.field.hierarchical
    if useComposite then {
      obj(
        "composite" -> obj(
          "size" -> num(facetChildCountPageSize),
          "sources" -> arr(
            obj("token" -> obj("terms" -> obj("field" -> str(tokenField))))
          )
        )
      )
    } else {
      val parts = Vector(
        Some("field" -> str(tokenField)),
        facetChildCountPrecisionThreshold.map(pt => "precision_threshold" -> num(pt))
      ).flatten
      obj("cardinality" -> obj(parts: _*))
    }
  }

  // Filtering is done in OpenSearchTransaction when parsing aggregation buckets.

  private def sorts2Dsl(sorts: List[Sort]): List[Json] = {
    sorts.map {
      case Sort.IndexOrder =>
        // IMPORTANT:
        // Do not sort on OpenSearch metadata field `_id`. Sorting on `_id` can trigger fielddata loading and trip
        // the OpenSearch fielddata circuit breaker on large indices.
        //
        // `__lightdb_id` is a keyword field (doc_values) and is safe for stable sorting/pagination.
        obj(InternalIdFieldName -> obj("order" -> str("asc")))
      case Sort.BestMatch(direction) =>
        val order = if direction == lightdb.SortDirection.Descending then "desc" else "asc"
        obj("_score" -> obj("order" -> str(order)))
      case s: Sort.ByField[_, _] =>
        val order = if s.direction == lightdb.SortDirection.Descending then "desc" else "asc"
        val field = s.field.asInstanceOf[Field[Doc, _]]
        val isIdLikeString =
          (field.rw.definition == DefType.Str || field.rw.definition.isInstanceOf[DefType.Enum]) &&
            !field.isTokenized &&
            field.name != "_id" &&
            field.name.endsWith("Id")

        if isIdLikeString then {
          // Prefer doc-values keyword sorting. This is fast and predictable.
          //
          // Legacy compatibility (optional): allow script sorts for indices missing `.keyword` mappings.
          // WARNING: script sorts can be extremely slow on large result sets.
          if allowScriptSorts then {
            val base = rewriteReservedIdFieldName(field.name)
            val keyword = s"$base.keyword"
            obj("_script" -> obj(
              "type" -> str("string"),
              "script" -> obj(
                "lang" -> str("painless"),
                "source" -> str(
                  "def k=params.k; def b=params.b; " +
                    "if (doc.containsKey(k) && doc[k].size()!=0) return doc[k].value; " +
                    "if (doc.containsKey(b) && doc[b].size()!=0) return doc[b].value; " +
                    "return '';"
                ),
                "params" -> obj(
                  "k" -> str(keyword),
                  "b" -> str(base)
                )
              ),
              "order" -> str(order)
            ))
          } else {
            val fieldName = sortFieldName(field)
            obj(fieldName -> obj("order" -> str(order)))
          }
        } else {
          val fieldName = sortFieldName(field)
          obj(fieldName -> obj("order" -> str(order)))
        }
      case s: Sort.ByDistance[_, _] =>
        val order = if s.direction == lightdb.SortDirection.Descending then "desc" else "asc"
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
      val must = if tokens.nonEmpty then tokens.map(t => OpenSearchDsl.term(field.name, str(t))) else List(OpenSearchDsl.matchAll())
      addBoost(OpenSearchDsl.boolQuery(must = must), boost.getOrElse(1.0))
    case Str(s, _) =>
      val base = rewriteReservedIdFieldName(field.name)
      val keyword = s"$base.keyword"
      val v = if keywordNormalize then normalizeKeyword(s) else s

      // Compatibility: match either keyword multifield (text+keyword mapping) or direct keyword field mapping.
      addBoost(
        OpenSearchDsl.boolQuery(
          should = List(
            OpenSearchDsl.term(keyword, str(v), boost = None),
            OpenSearchDsl.term(base, str(v), boost = None)
          ),
          minimumShouldMatch = Some(1)
        ),
        boost.getOrElse(1.0)
      )
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
      case o: Obj if o.value.size == 1 && o.value.contains("has_child") =>
        o.value("has_child") match {
          case hc: Obj =>
            // OpenSearch supports `boost` directly on has_child; avoid function_score for join performance.
            obj("has_child" -> obj((hc.value.toSeq :+ ("boost" -> num(boost))): _*))
          case _ =>
            functionScore(query, boost)
        }
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
    case _ if field.name == "_id" =>
      // `_id` is reserved in OpenSearch and is not present in `_source`; we store it in `__lightdb_id`.
      InternalIdFieldName
    case DefType.Str | DefType.Enum(_, _) =>
      s"${rewriteReservedIdFieldName(field.name)}.keyword"
    case DefType.Opt(d) =>
      sortFieldNameFromDef(rewriteReservedIdFieldName(field.name), d)
    case DefType.Arr(d) =>
      sortFieldNameFromDef(rewriteReservedIdFieldName(field.name), d)
    case _ =>
      rewriteReservedIdFieldName(field.name)
  }

  private def sortFieldNameFromDef(fieldName: String, d: DefType): String = d match {
    case _ if fieldName == "_id" => InternalIdFieldName
    case DefType.Str | DefType.Enum(_, _) => s"$fieldName.keyword"
    case DefType.Opt(inner) => sortFieldNameFromDef(fieldName, inner)
    case DefType.Arr(inner) => sortFieldNameFromDef(fieldName, inner)
    case _ => fieldName
  }

  private val regexChars = ".?+*|{}[]()\"\\#".toSet
  private def escapeRegexLiteral(s: String): String =
    s.flatMap(c => if regexChars.contains(c) then s"\\$c" else c.toString)

  private def fieldNameForExact(field: Field[Doc, _]): String = field.rw.definition match {
    case _ if field.name == "_id" =>
      InternalIdFieldName
    case DefType.Str | DefType.Enum(_, _) if !field.isInstanceOf[Tokenized[_]] =>
      s"${rewriteReservedIdFieldName(field.name)}.keyword"
    case DefType.Opt(d) =>
      fieldNameForExactFromDef(rewriteReservedIdFieldName(field.name), d, tokenized = field.isInstanceOf[Tokenized[_]])
    case DefType.Arr(d) =>
      fieldNameForExactFromDef(rewriteReservedIdFieldName(field.name), d, tokenized = field.isInstanceOf[Tokenized[_]])
    case _ =>
      rewriteReservedIdFieldName(field.name)
  }

  private def fieldNameForExactFromDef(fieldName: String, d: DefType, tokenized: Boolean): String = d match {
    case _ if fieldName == "_id" => InternalIdFieldName
    case DefType.Str | DefType.Enum(_, _) if !tokenized => s"$fieldName.keyword"
    case DefType.Opt(inner) => fieldNameForExactFromDef(fieldName, inner, tokenized)
    case DefType.Arr(inner) => fieldNameForExactFromDef(fieldName, inner, tokenized)
    case _ => fieldName
  }

  private def fieldNameForPattern(fieldName: String): String =
    if fieldName == "_id" then {
      InternalIdFieldName
    } else
    model.fields.find(_.name == fieldName) match {
      case Some(f) if f.isTokenized => fieldName
      case Some(f) if f.rw.definition == DefType.Str || f.rw.definition.isInstanceOf[DefType.Enum] =>
        s"$fieldName.keyword"
      case Some(f) => fieldNameForExact(f.asInstanceOf[Field[Doc, _]])
      case None => fieldName
    }
}


