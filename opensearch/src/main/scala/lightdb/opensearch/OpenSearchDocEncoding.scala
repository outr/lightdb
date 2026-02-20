package lightdb.opensearch

import fabric.*
import fabric.io.JsonFormatter
import fabric.rw.*
import lightdb.doc.Document
import lightdb.field.Field.FacetField
import lightdb.field.{Field, IndexingState}
import lightdb.facet.FacetValue
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.spatial.Geo

import scala.collection.mutable
import scala.util.Try

/**
 * Shared document encoding rules for OpenSearch.
 *
 * This is used both by transactional indexing and offline rebuild helpers so that:
 * - join-domain fields + routing behave the same
 * - facet/spatial derived fields are generated consistently
 * - `_id` escaping/unescaping stays correct
 */
object OpenSearchDocEncoding {
  case class PreparedIndex(source: Json, routing: Option[String])

  private val RootMarker: String = "$ROOT$"
  private val MissingMarker: String = "$MISSING$"

  private def shouldIndexParentField[Doc <: Document[Doc]](field: Field[Doc, _]): Boolean = field match {
    case _: Field.NestedIndex[?, ?] =>
      // OpenSearch nested queries execute against nested objects embedded in the parent document.
      // Unlike Lucene block-join indexing, OpenSearch has no separate nested-child storage path,
      // so we must always include nested fields in parent _source regardless of indexParent.
      true
    case _ => true
  }

  /**
   * Build the OpenSearch `_source` for a doc and compute routing (join-domain aware).
   */
  def prepareForIndexing[Doc <: Document[Doc]](doc: Doc,
                                               storeName: String,
                                               fields: List[Field[Doc, _]],
                                               config: OpenSearchConfig): PreparedIndex = {
    val state = new IndexingState
    var joinParentId: Option[String] = None

    val pairs = fields.iterator
      .filterNot(_.name == "_id")
      .filter(f => shouldIndexParentField(f))
      .map { f =>
        val j = f.getJson(doc, state)
        val normalized = escapeReservedIds(j)
        val value = normalized match {
          case _ if f.rw.definition == fabric.define.DefType.Json =>
            // Persist Json as a compact string to avoid mapping conflicts (KeyValue backing store, etc.)
            Str(JsonFormatter.Compact(normalized))
          case other =>
            other
        }
        if config.joinDomain.nonEmpty && config.joinRole.contains("child") && config.joinParentField.contains(f.name) then {
          joinParentId = value match {
            case Str(s, _) => Some(s)
            case other => Some(other.toString)
          }
        }
        f.name -> value
      }
      .toList

    val base = obj(((OpenSearchTemplates.InternalIdField, Str(doc._id.value)) :: pairs): _*)

    val withJoin = if config.joinDomain.nonEmpty then {
      config.joinRole match {
        case Some("parent") =>
          // OpenSearch join field expects the parent value to be a string (the join "type").
          // Child values are objects with { "name": "<childType>", "parent": "<parentId>" }.
          obj((base.asObj.value.toSeq :+ (config.joinFieldName -> str(storeName))): _*)
        case Some("child") =>
          val parentId = joinParentId.getOrElse(throw new RuntimeException(
            s"Missing joinParentField='${config.joinParentField.getOrElse("")}' value for child document in joinDomain='${config.joinDomain.getOrElse("")}'"
          ))
          val join = obj("name" -> str(storeName), "parent" -> str(parentId))
          obj((base.asObj.value.toSeq :+ (config.joinFieldName -> join)): _*)
        case _ =>
          base
      }
    } else {
      base
    }

    val source = normalizeDottedFieldNames(
      augmentFacetTokens(augmentSpatialCenters(withJoin, fields), doc, fields, state, config)
    )

    val routing = if config.joinDomain.nonEmpty then {
      config.joinRole match {
        case Some("parent") => Some(doc._id.value)
        case Some("child") => joinParentId
        case _ => None
      }
    } else {
      None
    }

    PreparedIndex(source = source, routing = routing)
  }

  /**
   * Remove OpenSearch internal fields and unescape nested `_id`s.
   */
  def stripInternalFields(source: Json, config: OpenSearchConfig): Json = source match {
    case o: Obj =>
      val cleaned = obj(o.value.toSeq.filterNot {
        case (k, _) =>
          k == OpenSearchTemplates.InternalIdField ||
            k == config.joinFieldName ||
            k.endsWith(OpenSearchTemplates.SpatialCenterSuffix)
      }: _*)
      unescapeReservedIds(cleaned)
    case other => unescapeReservedIds(other)
  }

  /**
   * Escape nested `_id` keys to avoid OpenSearch reserved handling.
   */
  def escapeReservedIds(json: Json): Json = json match {
    case o: Obj =>
      val updated = o.value.toSeq.map { case (k, v) =>
        val key = if k == "_id" then OpenSearchTemplates.InternalIdField else k
        key -> escapeReservedIds(v)
      }
      obj(updated: _*)
    case Arr(values, _) =>
      arr(values.map(escapeReservedIds): _*)
    case other => other
  }

  def unescapeReservedIds(json: Json): Json = json match {
    case o: Obj =>
      val updated = o.value.toSeq.map { case (k, v) =>
        val key = if k == OpenSearchTemplates.InternalIdField then "_id" else k
        key -> unescapeReservedIds(v)
      }
      obj(updated: _*)
    case Arr(values, _) =>
      arr(values.map(unescapeReservedIds): _*)
    case other => other
  }

  private def geoPoint(lat: Double, lon: Double): Json =
    obj("lat" -> num(lat), "lon" -> num(lon))

  private def augmentSpatialCenters(source: Json, fields: List[Field[_, _]]): Json = source match {
    case o: Obj =>
      val extras = fields.collect {
        case f if f.isSpatial && f.name != "_id" =>
          val fieldName = f.name
          val centerFieldName = s"$fieldName${OpenSearchTemplates.SpatialCenterSuffix}"
          val geos: List[Geo] = o.value.get(fieldName) match {
            case None => Nil
            case Some(Null) => Nil
            case Some(Arr(values, _)) =>
              values.toList.flatMap(j => Try(j.as[Geo]).toOption)
            case Some(j) =>
              Try(j.as[Geo]).toOption.toList
          }
          val centers: List[Json] = if geos.isEmpty then {
            // Match Lucene behavior: index a dummy point so docvalues exist, and exclude it at query-time.
            List(geoPoint(0.0, 0.0))
          } else {
            geos.map(g => geoPoint(g.center.latitude, g.center.longitude))
          }
          (centerFieldName, arr(centers: _*))
      }
      obj((o.value.toSeq ++ extras): _*)
    case other => other
  }

  private def augmentFacetTokens[Doc <: Document[Doc]](source: Json,
                                                       doc: Doc,
                                                       fields: List[Field[Doc, _]],
                                                       state: IndexingState,
                                                       config: OpenSearchConfig): Json = {
    val facetFields: List[FacetField[Doc]] = fields.collect {
      case ff: lightdb.field.Field.FacetField[_] =>
        ff.asInstanceOf[FacetField[Doc]]
    }
    if facetFields.isEmpty then {
      source
    } else {
      val extras = facetFields.map { ff =>
        val values: List[FacetValue] = ff.get(doc, ff, state)
        val tokens: List[String] = values.flatMap { fv =>
          val p = fv.path
          if ff.hierarchical then {
            if p.isEmpty then {
              List(RootMarker)
            } else {
              val prefixes = p.indices.map(i => p.take(i + 1).mkString("/")).toList
              val terminal = s"${p.mkString("/")}/$RootMarker"
              prefixes ::: List(terminal)
            }
          } else {
            if p.isEmpty then Nil else List(p.mkString("/"))
          }
        }.distinct
        val finalTokens = if tokens.isEmpty && config.facetIncludeMissing then {
          List(MissingMarker)
        } else {
          tokens
        }
        s"${ff.name}__facet" -> arr(finalTokens.map(str): _*)
      }
      source match {
        case o: Obj =>
          obj((o.value.toSeq ++ extras): _*)
        case _ => source
      }
    }
  }

  /**
   * OpenSearch treats dotted field names as object paths.
   *
   * When `_source` contains both a concrete object/array root (for example `outers`) and a dotted key under the same
   * root (for example `outers.children.code`), bulk indexing can fail with mapper parsing exceptions.
   *
   * Normalize top-level dotted keys into nested object structures and skip keys that conflict with an existing
   * non-object container (for example arrays).
   */
  private def normalizeDottedFieldNames(source: Json): Json = source match {
    case o: Obj =>
      val base = mutable.LinkedHashMap.empty[String, Json]
      val dotted = mutable.ListBuffer.empty[(String, Json)]
      o.value.foreach { case (k, v) =>
        if k.contains(".") then dotted += ((k, v)) else base.update(k, v)
      }

      def splitSegments(key: String): List[String] =
        key.split("\\.").toList.map(_.trim).filter(_.nonEmpty)

      def insertPath(map: mutable.LinkedHashMap[String, Json], segments: List[String], value: Json): Boolean =
        segments match {
          case Nil =>
            false
          case key :: Nil =>
            map.get(key) match {
              case None =>
                map.update(key, value)
                true
              case Some(Null) =>
                map.update(key, value)
                true
              case Some(_: Obj) =>
                false
              case Some(_) =>
                // Preserve explicit field values if both dotted and non-dotted names target the same key.
                false
            }
          case head :: tail =>
            map.get(head) match {
              case None | Some(Null) =>
                val child = mutable.LinkedHashMap.empty[String, Json]
                val ok = insertPath(child, tail, value)
                if ok then map.update(head, obj(child.toSeq: _*))
                ok
              case Some(existing: Obj) =>
                val child = mutable.LinkedHashMap.empty[String, Json]
                existing.value.foreach { case (k, v) => child.update(k, v) }
                val ok = insertPath(child, tail, value)
                if ok then map.update(head, obj(child.toSeq: _*))
                ok
              case Some(_: Arr) =>
                // Cannot inject object-path children into an existing array node.
                false
              case Some(_) =>
                false
            }
        }

      dotted.foreach { case (rawKey, value) =>
        val segments = splitSegments(rawKey)
        if segments.nonEmpty then {
          insertPath(base, segments, value)
        }
      }
      obj(base.toSeq: _*)
    case other =>
      other
  }
}


