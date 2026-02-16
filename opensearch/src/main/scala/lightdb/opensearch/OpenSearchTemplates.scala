package lightdb.opensearch

import fabric.*
import fabric.define.DefType
import fabric.io.JsonFormatter
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.field.Field
import lightdb.field.Field.Tokenized
import lightdb.opensearch.client.OpenSearchConfig

object OpenSearchTemplates {
  val SpatialCenterSuffix: String = "__center"
  // Stored in _source so we can do prefix queries / sort-by-id without depending on OpenSearch's metadata _id behavior.
  // Also used as an escape hatch for nested "_id" fields (which are problematic in OpenSearch).
  val InternalIdField: String = "__lightdb_id"
  val KeywordNormalizerName: String = "__lightdb_keyword_norm"

  /**
   * Default index body used for initial parity work.
   *
   * Notes:
   * - `max_result_window` is increased to support LightDB's offset-based streaming in the abstract specs.
   * - mappings are left dynamic initially; we will tighten types as we implement full parity.
   */
  def defaultIndexBody(maxResultWindow: Int = 250_000): Json = obj(
    "settings" -> obj(
      "index" -> obj(
        "max_result_window" -> num(maxResultWindow)
      )
    ),
    "mappings" -> obj(
      "dynamic" -> bool(true)
    )
  )

  def indexBody[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                   fields: List[Field[Doc, _]],
                                                                   config: OpenSearchConfig,
                                                                   storeName: String,
                                                                   maxResultWindow: Int = 250_000): Json = {
    val props = properties(model, fields, config, storeName)
    val hash = mappingHash(props)
    val hasNestedPaths = model.nestedPaths.nonEmpty
    // OpenSearch defaults `index.mapping.nested_objects.limit` to 10_000.
    // Some stores (for example aggregated UnifiedEntity docs) can exceed this per-document.
    // Apply an explicit limit when nested paths are declared.
    val nestedObjectsLimit = if hasNestedPaths then config.nestedObjectsLimit.orElse(Some(100_000)) else None
    val indexObjBase: List[(String, Json)] =
      List("max_result_window" -> num(maxResultWindow)) ::: nestedObjectsLimit.toList.map { limit =>
        "mapping.nested_objects.limit" -> num(limit)
      }

    val sortFields: List[String] = config.indexSortFields.map(_.trim).filter(_.nonEmpty)
    val sortOrdersRaw: List[String] = config.indexSortOrders.map(_.trim.toLowerCase).filter(_.nonEmpty)
    val sortOrders: List[String] =
      if sortFields.isEmpty then Nil
      else if sortOrdersRaw.isEmpty then List.fill(sortFields.length)("asc")
      else if sortOrdersRaw.length == 1 then List.fill(sortFields.length)(sortOrdersRaw.head)
      else if sortOrdersRaw.length >= sortFields.length then sortOrdersRaw.take(sortFields.length)
      else sortOrdersRaw ::: List.fill(sortFields.length - sortOrdersRaw.length)("asc")

    val indexObj: Json = if sortFields.nonEmpty then {
      obj((indexObjBase ++ List(
        "sort.field" -> arr(sortFields.map(str): _*),
        "sort.order" -> arr(sortOrders.map(str): _*)
      )): _*)
    } else {
      obj(indexObjBase: _*)
    }

    val settings = if config.keywordNormalize then {
      // Keyword fields (non-tokenized strings) are case-sensitive by default (Lucene parity).
      // When enabled, normalize keyword terms for both indexing and query literals.
      obj(
        "index" -> indexObj,
        "analysis" -> obj(
          "normalizer" -> obj(
            KeywordNormalizerName -> obj(
              "type" -> str("custom"),
              "filter" -> arr(str("trim"), str("lowercase"))
            )
          )
        )
      )
    } else {
      obj(
        "index" -> indexObj
      )
    }
    obj(
    "settings" -> settings,
    "mappings" -> obj(
      "dynamic" -> bool(true),
      "_meta" -> obj(
        "lightdb" -> obj(
          "mapping_hash" -> str(hash),
          "store" -> str(storeName)
        )
      ),
      "properties" -> props
    )
    )
  }

  private def properties[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                            fields: List[Field[Doc, _]],
                                                                            config: OpenSearchConfig,
                                                                            storeName: String): Json = {
    val nestedPaths = model.nestedPaths.toList.sorted
    def isMappedByNestedPath(fieldName: String): Boolean =
      nestedPaths.exists(path => fieldName == path || fieldName.startsWith(s"$path."))

    val baseProps = List(
      InternalIdField -> obj("type" -> str("keyword"))
    )
    val effectiveJoinChildren: List[String] = if config.joinChildren.nonEmpty then {
      config.joinChildren
    } else {
      model match {
        case pcs: ParentChildSupport[_, _, _] =>
          // If the parent model already declares its child store (common case), default the join mapping to that
          // single child store name to reduce config friction.
          List(pcs.childStore.name)
        case _ =>
          Nil
      }
    }
    val joinProps: List[(String, Json)] = if config.joinDomain.nonEmpty && config.joinRole.contains("parent") && effectiveJoinChildren.nonEmpty then {
      val children = arr(effectiveJoinChildren.map(str): _*)
      val relations = obj(storeName -> children)
      List(config.joinFieldName -> obj(
        "type" -> str("join"),
        "relations" -> relations
      ))
    } else {
      Nil
    }
    val props0 = fields.collect {
      case f if f.name == "_id" => Nil
      case f if f.isSpatial =>
        // Store the original GeoJSON in the field itself (dynamic mapping is fine),
        // but index a separate derived geo_point field for distance/sort parity.
        List(s"${f.name}$SpatialCenterSuffix" -> obj("type" -> str("geo_point")))
      case f if f.isInstanceOf[lightdb.field.Field.FacetField[_]] =>
        // Derived keyword tokens to support drill-down + hierarchical facet aggregation.
        // NOTE: do not apply keyword normalization here; facet tokens use reserved markers like "$ROOT$".
        List(s"${f.name}__facet" -> obj("type" -> str("keyword")))
      case f if isMappedByNestedPath(f.name) =>
        // Backed by a declared nested path. Let OpenSearch dynamically infer child field types under that nested mapping.
        Nil
      case f =>
        val mapping = mappingForField(f, config)
        // If we don't have an explicit mapping (empty object), omit the property and let OpenSearch infer it.
        if mapping.asObj.value.nonEmpty then List(f.name -> mapping) else Nil
    }.flatten

    val nestedLeafPropsFromIndexedFields = fields.collect {
      case f if isMappedByNestedPath(f.name) &&
        f.name.contains(".") &&
        f.name != "_id" &&
        !f.isSpatial &&
        !f.isInstanceOf[lightdb.field.Field.FacetField[_]] =>
        val mapping = mappingForField(f, config)
        if mapping.asObj.value.nonEmpty then Some(f.name -> mapping) else None
    }.flatten

    val nestedLeafPropsFromNestedRoots = nestedPaths.flatMap { nestedPath =>
      fields.find(_.name == nestedPath).toList.flatMap { f =>
        nestedLeafMappingsFromDef(nestedPath, f.rw.definition, config)
      }
    }

    // Merge discovered nested leaf mappings. Explicit indexed subfield mappings win.
    val nestedLeafProps = (nestedLeafPropsFromNestedRoots ++ nestedLeafPropsFromIndexedFields)
      .foldLeft(Map.empty[String, Json]) { case (acc, (name, mapping)) => acc.updated(name, mapping) }
      .toList
      .sortBy(_._1)

    // Index sorting requires sort fields to be present in the mapping at index creation time.
    // In join-domain indices, the join-parent's model may not declare child-only fields (ex: EntityRecord fields),
    // but users still may want to index-sort on them. Support this by injecting minimal mappings for configured
    // sort fields when they're missing.
    def injectedSortProps(existing: Map[String, Json]): List[(String, Json)] = {
      val sortFields = config.indexSortFields.map(_.trim).filter(_.nonEmpty)
      sortFields.flatMap {
        case f if f == InternalIdField =>
          Nil // already present
        case f if f.endsWith(".keyword") =>
          val base = f.stripSuffix(".keyword")
          if base.isEmpty || existing.contains(base) then Nil
          else {
            // Create a text+keyword multifield mapping so `<base>.keyword` exists for sort usage.
            val keyword = if config.keywordNormalize then {
              obj("type" -> str("keyword"), "normalizer" -> str(KeywordNormalizerName))
            } else {
              obj("type" -> str("keyword"))
            }
            List(base -> obj(
              "type" -> str("text"),
              "analyzer" -> str("standard"),
              "fields" -> obj("keyword" -> keyword)
            ))
          }
        case f =>
          // For non-multifield paths, only inject if it doesn't exist; map as keyword for safe sorting.
          if existing.contains(f) then Nil
          else List(f -> obj("type" -> str("keyword")))
      }
    }

    val nestedProps = nestedPathMappings(nestedPaths, nestedLeafProps)
    val baseMap = (baseProps ++ joinProps ++ nestedProps ++ props0).toMap
    val props = baseProps ++ joinProps ++ nestedProps ++ props0 ++ injectedSortProps(baseMap)
    obj(props: _*)
  }

  private def nestedPathMappings(paths: List[String], fieldMappings: List[(String, Json)]): List[(String, Json)] = {
    final class Node {
      var isNested: Boolean = false
      val children = scala.collection.mutable.Map.empty[String, Node]
      val leafMappings = scala.collection.mutable.Map.empty[String, Json]
    }

    val root = new Node
    paths.foreach { rawPath =>
      val segments = rawPath.split("\\.").toList.map(_.trim).filter(_.nonEmpty)
      if segments.nonEmpty then {
        var node = root
        segments.foreach { segment =>
          node = node.children.getOrElseUpdate(segment, new Node)
        }
        node.isNested = true
      }
    }

    fieldMappings.foreach { case (rawFieldName, mapping) =>
      val segments = rawFieldName.split("\\.").toList.map(_.trim).filter(_.nonEmpty)
      if segments.nonEmpty then {
        var node = root
        segments.dropRight(1).foreach { segment =>
          node = node.children.getOrElseUpdate(segment, new Node)
        }
        val leaf = segments.last
        // Do not overwrite an existing object/nested node at the same key.
        if !node.children.contains(leaf) then {
          node.leafMappings.update(leaf, mapping)
        }
      }
    }

    def toJson(node: Node): Json = {
      val base = if node.isNested then {
        obj(
          "type" -> str("nested"),
          "dynamic" -> bool(true)
        )
      } else {
        obj(
          "type" -> str("object"),
          "dynamic" -> bool(true)
        )
      }
      val children = node.children.toList.sortBy(_._1).map {
        case (name, child) => name -> toJson(child)
      }
      val leaves = node.leafMappings.toList.sortBy(_._1)
      val props = children ++ leaves
      if props.isEmpty then {
        base
      } else {
        obj((base.asObj.value.toSeq :+ ("properties" -> obj(props: _*))): _*)
      }
    }

    root.children.toList.sortBy(_._1).map {
      case (name, node) => name -> toJson(node)
    }
  }

  private def mappingForField[Doc <: Document[Doc]](field: Field[Doc, _], config: OpenSearchConfig): Json = field.rw.definition match {
    case DefType.Str | DefType.Enum(_, _) =>
      stringMapping(field, config)
    case DefType.Bool =>
      obj("type" -> str("boolean"))
    case DefType.Int =>
      obj("type" -> str("long"))
    case DefType.Dec =>
      obj("type" -> str("double"))
    case DefType.Opt(d) =>
      mappingForDefType(field, d, config)
    case DefType.Arr(d) =>
      mappingForDefType(field, d, config)
    case DefType.Obj(_, _) | DefType.Poly(_, _) =>
      // Store structured JSON for materialization/conversion parity.
      obj("type" -> str("object"), "dynamic" -> bool(true))
    case DefType.Json =>
      // Json fields can vary between scalar/array/object across documents; dynamic typing will conflict.
      // Store as a compact JSON string (decoded at read-time when needed).
      obj("type" -> str("keyword"))
    case _ =>
      // Fallback to dynamic mapping.
      obj()
  }

  private def stringMapping[Doc <: Document[Doc]](field: Field[Doc, _], config: OpenSearchConfig): Json = {
    val keyword = if config.keywordNormalize then {
      obj("type" -> str("keyword"), "normalizer" -> str(KeywordNormalizerName))
    } else {
      obj("type" -> str("keyword"))
    }

    if field.isInstanceOf[Tokenized[_]] then {
      // Tokenized fields (ex: fullText) can be extremely large. Adding a `.keyword` subfield causes OpenSearch to
      // attempt to index the entire string as a single keyword term, which can exceed the Lucene max term length
      // (32766 bytes) and fail bulk indexing with "immense term" errors.
      //
      // LuceneStore doesn't need an "exact" keyword subfield for tokenized fulltext; it indexes terms for searching.
      // Match that intent here by mapping tokenized fields as `text` only.
      obj(
        "type" -> str("text"),
        "analyzer" -> str("standard")
      )
    } else {
      // Keep a text+keyword multifield mapping for non-tokenized strings for backward compatibility with
      // existing indices and existing query/sort behavior (e.g. sorting on `<field>.keyword`).
      obj(
        "type" -> str("text"),
        "analyzer" -> str("standard"),
        "fields" -> obj(
          "keyword" -> keyword
        )
      )
    }
  }

  private def mappingForDefType[Doc <: Document[Doc]](field: Field[Doc, _], d: DefType, config: OpenSearchConfig): Json = d match {
    case DefType.Str | DefType.Enum(_, _) => stringMapping(field, config)
    case DefType.Bool => obj("type" -> str("boolean"))
    case DefType.Int => obj("type" -> str("long"))
    case DefType.Dec => obj("type" -> str("double"))
    case DefType.Opt(inner) => mappingForDefType(field, inner, config)
    case DefType.Arr(inner) => mappingForDefType(field, inner, config)
    case DefType.Obj(_, _) | DefType.Poly(_, _) => obj("type" -> str("object"), "dynamic" -> bool(true))
    case DefType.Json => obj("type" -> str("keyword"))
    case _ => obj()
  }

  private def nestedLeafMappingsFromDef(path: String, definition: DefType, config: OpenSearchConfig): List[(String, Json)] = {
    def recurse(prefix: String, t: DefType): List[(String, Json)] = t match {
      case DefType.Opt(inner) =>
        recurse(prefix, inner)
      case DefType.Arr(inner) =>
        inner match {
          case DefType.Obj(map, _) =>
            map.toList.flatMap { case (name, dt) => recurse(s"$prefix.$name", dt) }
          case other =>
            val mapping = nestedScalarMapping(other, config)
            if mapping.asObj.value.nonEmpty then List(prefix -> mapping) else Nil
        }
      case DefType.Obj(map, _) =>
        map.toList.flatMap { case (name, dt) => recurse(s"$prefix.$name", dt) }
      case other =>
        val mapping = nestedScalarMapping(other, config)
        if mapping.asObj.value.nonEmpty then List(prefix -> mapping) else Nil
    }

    recurse(path, definition)
  }

  private def nestedScalarMapping(d: DefType, config: OpenSearchConfig): Json = d match {
    case DefType.Str | DefType.Enum(_, _) =>
      val keyword = if config.keywordNormalize then {
        obj("type" -> str("keyword"), "normalizer" -> str(KeywordNormalizerName))
      } else {
        obj("type" -> str("keyword"))
      }
      obj(
        "type" -> str("text"),
        "analyzer" -> str("standard"),
        "fields" -> obj("keyword" -> keyword)
      )
    case DefType.Bool =>
      obj("type" -> str("boolean"))
    case DefType.Int =>
      obj("type" -> str("long"))
    case DefType.Dec =>
      obj("type" -> str("double"))
    case DefType.Opt(inner) =>
      nestedScalarMapping(inner, config)
    case DefType.Arr(inner) =>
      nestedScalarMapping(inner, config)
    case DefType.Obj(_, _) | DefType.Poly(_, _) =>
      obj("type" -> str("object"), "dynamic" -> bool(true))
    case DefType.Json =>
      obj("type" -> str("keyword"))
    case _ =>
      obj()
  }

  /**
   * Canonicalize Json by sorting object keys recursively. This is used to compute stable mapping hashes even if
   * OpenSearch reorders mapping keys.
   */
  private def canonical(json: Json): Json = json match {
    case o: Obj =>
      val sorted = o.value.toList.sortBy(_._1).map { case (k, v) => k -> canonical(v) }
      obj(sorted: _*)
    case Arr(values, _) =>
      arr(values.map(canonical): _*)
    case other =>
      other
  }

  private def mappingHash(properties: Json): String = {
    val canonicalJson = canonical(properties)
    val bytes = JsonFormatter.Compact(canonicalJson).getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val md = java.security.MessageDigest.getInstance("SHA-256")
    val digest = md.digest(bytes)
    digest.map("%02x".format(_)).mkString
  }
}


