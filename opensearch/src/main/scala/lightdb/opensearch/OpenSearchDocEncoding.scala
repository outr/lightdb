package lightdb.opensearch

import fabric._
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.doc.Document
import lightdb.field.Field.FacetField
import lightdb.field.{Field, IndexingState}
import lightdb.facet.FacetValue
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.spatial.Geo

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

    val source = augmentFacetTokens(augmentSpatialCenters(withJoin, fields), doc, fields, state, config)

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
}


