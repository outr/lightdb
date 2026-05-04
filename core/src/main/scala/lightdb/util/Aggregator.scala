package lightdb.util

import fabric.rw.*
import fabric.{Json, Null, NumDec, NumInt, Obj, num}
import lightdb.SortDirection.Ascending
import lightdb.aggregate.{AggregateFunction, AggregateQuery, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.materialized.{MaterializedAggregate, MaterializedIndex}
import rapid.Task

import scala.collection.mutable

/**
 * Convenience class to stream aggregation for Stores that don't directly support aggregation.
 *
 * Honors:
 *   - Multiple [[AggregateType.Group]] functions (composite outer-bucket key)
 *   - Per-bucket [[AggregateFunction.subAggregates]] (recursive — sub-aggregates may themselves
 *     be Groups whose sub-aggregates produce a third level)
 *   - [[AggregateQuery.limit]] (applied after sort)
 *
 * For sub-aggregations to work, records that fall into each outer bucket are buffered (as
 * lightweight projected `Materialized` rows holding only the requested fields) and re-folded
 * with the sub-aggregate function list. This is correct but allocates per-bucket buffers; the
 * fallback is acceptable for backends that don't expose a native aggregation API. Backends with
 * native aggs (OpenSearch, SQL flat) should bypass this for the high-cardinality path.
 */
object Aggregator {
  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](query: AggregateQuery[Doc, Model], model: Model): rapid.Stream[MaterializedAggregate[Doc, Model]] = {
    // Union of fields needed across the top-level functions and any nested sub-aggregates.
    // We have to collect early so we materialize the whole projection in one stream pass —
    // sub-aggregate fields wouldn't otherwise be available to the per-bucket fold.
    val fields = collectAllFields(query.functions).distinct
    val stream = query.query.materialized(_ => fields).stream
    val anySubAggs = query.functions.exists(_.subAggregates.nonEmpty)

    val groups = mutable.LinkedHashMap.empty[List[Any], Map[String, Json]]
    // Per-outer-bucket record buffer; only populated when at least one top-level function has
    // sub-aggregates. Avoids the buffering cost when nobody asked for nested aggs.
    val groupRecords =
      if (anySubAggs) Some(mutable.LinkedHashMap.empty[List[Any], mutable.ArrayBuffer[MaterializedIndex[Doc, Model]]])
      else None
    val groupFields = query.functions.filter(_.`type` == AggregateType.Group).map(_.field)

    stream
      .foreach { m =>
        val group = groupFields.map(f => m(_ => f.asInstanceOf[Field[Doc, Any]]))
        groups.update(group, foldOne(query.functions, m, groups.getOrElse(group, Map.empty)))
        groupRecords.foreach { gr =>
          gr.getOrElseUpdate(group, mutable.ArrayBuffer.empty) += m
        }
      }
      .drain
      .sync()

    val finalized = finalizeGroupValues(query.functions, groups)
    var list = finalized.toList.map { case (key, jsonMap) =>
      val subResults: Map[String, List[MaterializedAggregate[Doc, Model]]] = groupRecords match {
        case Some(gr) => buildSubResults(query.functions, gr.getOrElse(key, mutable.ArrayBuffer.empty), model)
        case None => Map.empty
      }
      MaterializedAggregate[Doc, Model](Obj(jsonMap), model, subResults)
    }
    query.sort.reverse.foreach {
      case (f, direction) =>
        list = list.sortBy(_.json(f.name))(if direction == Ascending then Json.JsonOrdering else Json.JsonOrdering.reverse)
    }
    query.limit.foreach(n => list = list.take(n))
    rapid.Stream.fromIterator(Task(list.iterator))
  }

  /** Walk the tree of functions + sub-aggregates and collect every field referenced. */
  private def collectAllFields[Doc <: Document[Doc]](funcs: List[AggregateFunction[_, _, Doc]]): List[Field[Doc, _]] =
    funcs.flatMap { f => f.field :: collectAllFields(f.subAggregates) }

  /** Apply one record's contributions to the running per-function accumulators for a bucket. */
  private def foldOne[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      funcs: List[AggregateFunction[_, _, Doc]],
      m: MaterializedIndex[Doc, Model],
      currentMap: Map[String, Json]
  ): Map[String, Json] = {
    var map = currentMap
    funcs.foreach { f =>
      val current = map.get(f.name)
      val value = m.value(_ => f.field)
      val newValue: Json = f.`type` match {
        case AggregateType.Max => value match {
          case NumInt(l, _) => current match {
            case Some(c) => num(math.max(c.asLong, l))
            case None => num(l)
          }
          case NumDec(bd, _) => current match {
            case Some(c) => num(bd.max(c.asBigDecimal))
            case None => num(bd)
          }
          case Null => current.getOrElse(Null)
          case _ => throw new UnsupportedOperationException(s"Unsupported type for Max: $value (${f.field.name})")
        }
        case AggregateType.Min => value match {
          case NumInt(l, _) => current match {
            case Some(c) => num(math.min(c.asLong, l))
            case None => num(l)
          }
          case NumDec(bd, _) => current match {
            case Some(c) => num(bd.min(c.asBigDecimal))
            case None => num(bd)
          }
          case Null => current.getOrElse(Null)
          case _ => throw new UnsupportedOperationException(s"Unsupported type for Min: $value (${f.field.name})")
        }
        case AggregateType.Avg =>
          val v = value.asBigDecimal
          current match {
            case Some(c) => (v :: c.as[List[BigDecimal]]).json
            case None => List(v).json
          }
        case AggregateType.Sum => value match {
          case NumInt(l, _) => current match {
            case Some(c) => num(c.asLong + l)
            case None => num(l)
          }
          case NumDec(bd, _) => current match {
            case Some(c) => num(bd + c.asBigDecimal)
            case None => num(bd)
          }
          case Null => current.getOrElse(Null)
          case _ => throw new UnsupportedOperationException(s"Unsupported type for Sum: $value (${f.field.name})")
        }
        case AggregateType.Count => current match {
          case Some(c) => num(c.asInt + 1)
          case None => num(0)
        }
        case AggregateType.CountDistinct | AggregateType.ConcatDistinct => current match {
          case Some(c) => (c.as[Set[Json]] + value).json
          case None => Set(value).json
        }
        case AggregateType.Group => value
        case AggregateType.Concat => current match {
          // Preserve encounter order (stream order)
          case Some(c) => (c.as[List[Json]] :+ value).json
          case None => List(value).json
        }
      }
      if newValue != Null then {
        map += f.name -> newValue
      }
    }
    map
  }

  /** Resolve final values for accumulators that hold intermediate state (Avg, CountDistinct). */
  private def finalizeGroupValues[Doc <: Document[Doc]](
      funcs: List[AggregateFunction[_, _, Doc]],
      groups: collection.Map[List[Any], Map[String, Json]]
  ): collection.Map[List[Any], Map[String, Json]] = groups.map {
    case (key, jsonMap) =>
      var map = jsonMap
      funcs.filter(_.`type` == AggregateType.Avg).foreach { f =>
        map.get(f.name).foreach { json =>
          val list = json.as[List[BigDecimal]]
          val avg = list.foldLeft(BigDecimal(0) -> 1) ((acc, i) => (acc._1 + (i - acc._1) / acc._2, acc._2 + 1))._1
          map += f.name -> num(avg)
        }
      }
      funcs.filter(_.`type` == AggregateType.CountDistinct).foreach { f =>
        map.get(f.name).foreach { json =>
          val set = json.as[Set[Json]]
          map += f.name -> num(set.size)
        }
      }
      key -> map
  }

  /**
   * For each top-level function with declared sub-aggregates, run an inner aggregation over the
   * outer bucket's collected records and produce one entry in the result `subResults` map.
   *
   * Non-Group sub-aggregates (e.g. a Sum or Count attached directly to a Group's `subAggregates`)
   * collapse to a single inner bucket — a single `MaterializedAggregate` whose JSON contains
   * the computed value(s).
   *
   * Group sub-aggregates produce one inner bucket per distinct sub-key, recursively.
   */
  private def buildSubResults[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      funcs: List[AggregateFunction[_, _, Doc]],
      records: collection.Seq[MaterializedIndex[Doc, Model]],
      model: Model
  ): Map[String, List[MaterializedAggregate[Doc, Model]]] = {
    val out = mutable.LinkedHashMap.empty[String, List[MaterializedAggregate[Doc, Model]]]
    funcs.filter(_.subAggregates.nonEmpty).foreach { parent =>
      val innerFuncs = parent.subAggregates
      val innerGroupFields = innerFuncs.filter(_.`type` == AggregateType.Group).map(_.field)
      if (innerGroupFields.isEmpty) {
        // Flat sub-aggregates — single inner bucket aggregated over all the outer bucket's records.
        var map = Map.empty[String, Json]
        records.foreach(m => map = foldOne(innerFuncs, m, map))
        val finalized = finalizeGroupValues(innerFuncs, Map(Nil -> map)).head._2
        out(parent.name) = List(MaterializedAggregate[Doc, Model](Obj(finalized), model))
      } else {
        val innerGroups = mutable.LinkedHashMap.empty[List[Any], Map[String, Json]]
        val innerRecords =
          if (innerFuncs.exists(_.subAggregates.nonEmpty))
            Some(mutable.LinkedHashMap.empty[List[Any], mutable.ArrayBuffer[MaterializedIndex[Doc, Model]]])
          else None
        records.foreach { m =>
          val innerKey = innerGroupFields.map(f => m(_ => f.asInstanceOf[Field[Doc, Any]]))
          innerGroups.update(innerKey, foldOne(innerFuncs, m, innerGroups.getOrElse(innerKey, Map.empty)))
          innerRecords.foreach(_.getOrElseUpdate(innerKey, mutable.ArrayBuffer.empty) += m)
        }
        val finalized = finalizeGroupValues(innerFuncs, innerGroups)
        out(parent.name) = finalized.toList.map { case (key, jsonMap) =>
          val nestedSubs: Map[String, List[MaterializedAggregate[Doc, Model]]] = innerRecords match {
            case Some(ir) => buildSubResults(innerFuncs, ir.getOrElse(key, mutable.ArrayBuffer.empty), model)
            case None => Map.empty
          }
          MaterializedAggregate[Doc, Model](Obj(jsonMap), model, nestedSubs)
        }
      }
    }
    out.toMap
  }
}