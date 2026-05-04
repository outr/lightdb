package lightdb.opensearch.query

import fabric.*
import lightdb.aggregate.{AggregateFunction, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate

/**
 * Translates a flat list of [[AggregateFunction]]s (with optional sub-aggregates) into an
 * OpenSearch `aggregations` request block, and parses the corresponding response back into
 * [[MaterializedAggregate]] trees.
 *
 * Translation rules — applied recursively at every level:
 *   - At most ONE [[AggregateType.Group]] function per level (becomes a `terms` agg).
 *     Multiple Groups at the same level would require composite/multi_terms; not translated
 *     here, caller falls back to [[lightdb.util.Aggregator]].
 *   - Metric siblings (`Sum`/`Max`/`Min`/`Avg`/`Count`/`CountDistinct`) become the matching
 *     metric agg, scoped to the enclosing bucket (or top-level if no Group at this level).
 *   - `Concat`/`ConcatDistinct` have no direct OS metric agg; presence anywhere in the tree
 *     marks the whole request non-translatable.
 *
 * Result shape — for each Group level, one [[MaterializedAggregate]] per `terms` bucket; the
 * Group's `name` field holds the bucket key, the `count`-aliased field (if any) holds
 * `doc_count`, and metric agg values are read from the bucket's named sub-results. Nested
 * sub-aggregates produce entries in [[MaterializedAggregate.subResults]] keyed by the sub-agg
 * function name.
 */
object OpenSearchAggTranslator {

  /** True if every function (and their sub-aggregates, recursively) maps to a known OS agg. */
  def isTranslatable[Doc <: Document[Doc]](funcs: List[AggregateFunction[_, _, Doc]]): Boolean = {
    val groupCount = funcs.count(_.`type` == AggregateType.Group)
    if (groupCount > 1) return false
    funcs.forall { f =>
      val typeOk = f.`type` match {
        case AggregateType.Concat | AggregateType.ConcatDistinct => false
        case _ => true
      }
      typeOk && isTranslatable(f.subAggregates)
    }
  }

  /**
   * Build the JSON value that goes under the request's `"aggregations"` key. Caller is
   * responsible for the outer envelope and the query body.
   *
   * `topLimit` is applied to the top-level `terms` agg as `size:`, so OpenSearch returns at
   * most that many outer buckets directly. Inner Group sub-aggs use a default size of 65 536
   * (OpenSearch's standard upper bound for safe terms aggs); callers needing a different inner
   * cap can express it via the [[AggregateFunction.subAggregates]] structure (extension point
   * for future tuning).
   */
  def buildAggsJson[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      sb: OpenSearchSearchBuilder[Doc, Model],
      funcs: List[AggregateFunction[_, _, Doc]],
      topLimit: Option[Int]
  ): Json = buildLevel(sb, funcs, topLimit)

  private def buildLevel[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      sb: OpenSearchSearchBuilder[Doc, Model],
      funcs: List[AggregateFunction[_, _, Doc]],
      sizeForGroup: Option[Int]
  ): Json = {
    val groupOpt = funcs.find(_.`type` == AggregateType.Group)
    val metricFuncs = funcs.filter(f => f.`type` != AggregateType.Group)

    groupOpt match {
      case Some(group) =>
        // One terms agg keyed by the group's field. Sibling metrics + the group's own
        // sub-aggregates all become `aggs:` of that terms agg (so they compute per bucket).
        val fieldName = sb.exactFieldName(group.field.asInstanceOf[lightdb.field.Field[Doc, _]])
        val termsBlock = obj(
          "field" -> str(fieldName),
          "size" -> num(sizeForGroup.getOrElse(DefaultInnerSize).toLong)
        )
        val perBucketFuncs = metricFuncs ::: group.subAggregates
        val perBucketAggs = aggsBlockFor(sb, perBucketFuncs)
        val termsAgg = if (perBucketAggs.value.nonEmpty) {
          obj("terms" -> termsBlock, "aggs" -> perBucketAggs)
        } else {
          obj("terms" -> termsBlock)
        }
        obj(group.name -> termsAgg)
      case None =>
        // No group at this level — every function emits a top-level metric agg.
        aggsBlockFor(sb, metricFuncs)
    }
  }

  /** Build an `aggs:` object holding metric/group aggs for each function in the list. */
  private def aggsBlockFor[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      sb: OpenSearchSearchBuilder[Doc, Model],
      funcs: List[AggregateFunction[_, _, Doc]]
  ): Obj = {
    val entries = funcs.flatMap { f =>
      f.`type` match {
        case AggregateType.Group =>
          // Nested Group — emit a terms sub-agg with this group's own metrics + sub-groups.
          val fieldName = sb.exactFieldName(f.field.asInstanceOf[lightdb.field.Field[Doc, _]])
          val perBucketAggs = aggsBlockFor(sb, f.subAggregates)
          val termsBlock = obj("field" -> str(fieldName), "size" -> num(DefaultInnerSize.toLong))
          val agg = if (perBucketAggs.value.nonEmpty) {
            obj("terms" -> termsBlock, "aggs" -> perBucketAggs)
          } else {
            obj("terms" -> termsBlock)
          }
          List(f.name -> agg)
        case AggregateType.Sum =>
          List(f.name -> obj("sum" -> obj("field" -> str(sb.exactFieldName(f.field)))))
        case AggregateType.Max =>
          List(f.name -> obj("max" -> obj("field" -> str(sb.exactFieldName(f.field)))))
        case AggregateType.Min =>
          List(f.name -> obj("min" -> obj("field" -> str(sb.exactFieldName(f.field)))))
        case AggregateType.Avg =>
          List(f.name -> obj("avg" -> obj("field" -> str(sb.exactFieldName(f.field)))))
        case AggregateType.CountDistinct =>
          List(f.name -> obj("cardinality" -> obj("field" -> str(sb.exactFieldName(f.field)))))
        case AggregateType.Count =>
          // doc_count is automatic on the enclosing bucket; we read it during parse and assign
          // it to this function's name. No agg block needed.
          Nil
        case AggregateType.Concat | AggregateType.ConcatDistinct =>
          // Should not happen — `isTranslatable` blocks translation when these are present.
          throw new UnsupportedOperationException(s"Cannot translate ${f.`type`} to OpenSearch agg")
      }
    }
    Obj(entries.toMap)
  }

  /**
   * Walk an OpenSearch `aggregations` response object and produce one [[MaterializedAggregate]]
   * per top-level outer bucket (or one synthetic bucket containing the global metrics, when no
   * top-level Group was requested).
   */
  def parseResults[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      aggsObj: Map[String, Json],
      funcs: List[AggregateFunction[_, _, Doc]],
      model: Model
  ): List[MaterializedAggregate[Doc, Model]] = {
    val groupOpt = funcs.find(_.`type` == AggregateType.Group)
    val metricFuncs = funcs.filter(f => f.`type` != AggregateType.Group)
    groupOpt match {
      case Some(group) =>
        val termsValue = aggsObj.getOrElse(group.name, obj()).asObj.value
        val buckets = termsValue.get("buckets").map(_.asVector.toList).getOrElse(Nil)
        buckets.map { bucket =>
          val bObj = bucket.asObj.value
          // Emit the bucket as a flat MaterializedAggregate carrying the group's key + the
          // metric values from the per-bucket aggs and the doc_count. Sub-aggregations that
          // were declared on the group flow into `subResults`.
          val docCount = bObj.get("doc_count").map(_.asLong).getOrElse(0L)
          val keyJson = bObj.getOrElse("key", Null)
          var rowMap = Map.empty[String, Json]
          rowMap += group.name -> keyJson
          // Sibling metrics + the group's own sub-aggs that were emitted as per-bucket aggs.
          val perBucketFuncs = metricFuncs ::: group.subAggregates
          rowMap = readMetricValues(perBucketFuncs, bObj, rowMap, docCount)
          val nestedSubs = readNestedSubResults(group.subAggregates, bObj, model)
          MaterializedAggregate[Doc, Model](Obj(rowMap), model, nestedSubs)
        }
      case None =>
        // No top-level Group — produce a single synthetic bucket holding the global metric
        // values. doc_count isn't directly available without track_total_hits; Count
        // aggregations without a Group are effectively undefined here, so we fill 0 and
        // leave that as a known limitation.
        var rowMap = Map.empty[String, Json]
        rowMap = readMetricValues(metricFuncs, aggsObj.toMap, rowMap, 0L)
        if (rowMap.isEmpty) Nil
        else List(MaterializedAggregate[Doc, Model](Obj(rowMap), model))
    }
  }

  /** Walk metric funcs and pull their values from the bucket / aggs object. */
  private def readMetricValues[Doc <: Document[Doc]](
      funcs: List[AggregateFunction[_, _, Doc]],
      bucket: Map[String, Json],
      seed: Map[String, Json],
      docCount: Long
  ): Map[String, Json] = funcs.foldLeft(seed) { (acc, f) =>
    f.`type` match {
      case AggregateType.Count =>
        acc + (f.name -> num(docCount))
      case AggregateType.Sum | AggregateType.Max | AggregateType.Min | AggregateType.Avg | AggregateType.CountDistinct =>
        bucket.get(f.name).flatMap(_.asObj.value.get("value")) match {
          case Some(v) => acc + (f.name -> v)
          case None    => acc
        }
      case AggregateType.Group =>
        // Already handled at this level by parseResults / per-bucket walk; nested groups go
        // through readNestedSubResults so we don't duplicate.
        acc
      case AggregateType.Concat | AggregateType.ConcatDistinct =>
        // Blocked by isTranslatable; defensive no-op.
        acc
    }
  }

  /**
   * For each sub-aggregate whose presence implies a nested bucket structure (i.e. another
   * `Group`), parse its bucket list under the current outer bucket and recurse.
   *
   * Non-Group sub-aggregates are surfaced as single-element lists holding a synthetic bucket
   * — that mirrors the in-memory Aggregator's shape, where flat sub-aggs collapse to one
   * inner row.
   */
  private def readNestedSubResults[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      subFuncs: List[AggregateFunction[_, _, Doc]],
      bucket: Map[String, Json],
      model: Model
  ): Map[String, List[MaterializedAggregate[Doc, Model]]] = {
    val out = scala.collection.mutable.LinkedHashMap.empty[String, List[MaterializedAggregate[Doc, Model]]]
    subFuncs.foreach { sf =>
      sf.`type` match {
        case AggregateType.Group =>
          val termsValue = bucket.get(sf.name).map(_.asObj.value).getOrElse(Map.empty[String, Json])
          val buckets = termsValue.get("buckets").map(_.asVector.toList).getOrElse(Nil)
          val parsed = buckets.map { b =>
            val bObj = b.asObj.value
            val docCount = bObj.get("doc_count").map(_.asLong).getOrElse(0L)
            val keyJson = bObj.getOrElse("key", Null)
            var rowMap = Map.empty[String, Json]
            rowMap += sf.name -> keyJson
            // Group-attached metrics live in the per-bucket aggs of this sub-bucket.
            rowMap = readMetricValues(sf.subAggregates, bObj, rowMap, docCount)
            val deeper = readNestedSubResults(sf.subAggregates, bObj, model)
            MaterializedAggregate[Doc, Model](Obj(rowMap), model, deeper)
          }
          out(sf.name) = parsed
        case _ => () // Flat metric sub-aggs are read into the parent bucket's row; nothing to nest.
      }
    }
    out.toMap
  }

  /** OpenSearch's safe upper bound for inner terms aggs; matches the convention used elsewhere. */
  private val DefaultInnerSize: Int = 65_536
}
