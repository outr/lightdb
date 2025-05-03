package lightdb.util

import fabric.rw._
import fabric.{Json, Null, NumDec, NumInt, Obj, num}
import lightdb.SortDirection.Ascending
import lightdb.aggregate.{AggregateQuery, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import rapid.Task

/**
 * Convenience class to stream aggregation for Stores that don't directly support aggregation
 */
object Aggregator {
  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](query: AggregateQuery[Doc, Model], model: Model): rapid.Stream[MaterializedAggregate[Doc, Model]] = {
    val fields = query.functions.map(_.field).distinct
    val groupFields = query.functions.filter(_.`type` == AggregateType.Group).map(_.field)
    val stream = query.query.materialized(_ => fields).stream
    var groups = Map.empty[List[Any], Map[String, Json]]
    stream
      .foreach { m =>
        val group = groupFields.map(f => m(_ => f.asInstanceOf[Field[Doc, Any]]))
        var map = groups.getOrElse(group, Map.empty)
        query.functions.foreach { f =>
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
              case Some(c) => (value :: c.as[List[Json]]).json
              case None => List(value).json
            }
          }
          if (newValue != Null) {
            map += f.name -> newValue
          }
        }
        groups += group -> map
      }
      .drain
      .sync()
    groups = groups.map {
      case (key, jsonMap) =>
        var map = jsonMap
        // Average
        query.functions.filter(_.`type` == AggregateType.Avg).foreach { f =>
          map.get(f.name).foreach { json =>
            val list = json.as[List[BigDecimal]]
            val avg = list.foldLeft(BigDecimal(0) -> 1) ((acc, i) => (acc._1 + (i - acc._1) / acc._2, acc._2 + 1))._1
            map += f.name -> num(avg)
          }
        }
        // CountDistinct
        query.functions.filter(_.`type` == AggregateType.CountDistinct).foreach { f =>
          map.get(f.name).foreach { json =>
            val set = json.as[Set[Json]]
            map += f.name -> num(set.size)
          }
        }
        key -> map
    }
    var list = groups.toList.map(t => MaterializedAggregate[Doc, Model](Obj(t._2), model))
    query.sort.reverse.foreach {
      case (f, direction) =>
        list = list.sortBy(_.json(f.name))(if (direction == Ascending) Json.JsonOrdering else Json.JsonOrdering.reverse)
    }
    rapid.Stream.fromIterator(Task(list.iterator))
  }
}