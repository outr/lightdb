package lightdb.opensearch

import fabric._
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
    val settings = if (config.keywordNormalize) {
      // Keyword fields (non-tokenized strings) are case-sensitive by default (Lucene parity).
      // When enabled, normalize keyword terms for both indexing and query literals.
      obj(
        "index" -> obj(
          "max_result_window" -> num(maxResultWindow)
        ),
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
        "index" -> obj(
          "max_result_window" -> num(maxResultWindow)
        )
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
    val baseProps = List(
      InternalIdField -> obj("type" -> str("keyword"))
    )
    val effectiveJoinChildren: List[String] = if (config.joinChildren.nonEmpty) {
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
    val joinProps: List[(String, Json)] = if (config.joinDomain.nonEmpty && config.joinRole.contains("parent") && effectiveJoinChildren.nonEmpty) {
      val children = arr(effectiveJoinChildren.map(str): _*)
      val relations = obj(storeName -> children)
      List(config.joinFieldName -> obj(
        "type" -> str("join"),
        "relations" -> relations
      ))
    } else {
      Nil
    }
    val props = fields.collect {
      case f if f.name == "_id" => Nil
      case f if f.isSpatial =>
        // Store the original GeoJSON in the field itself (dynamic mapping is fine),
        // but index a separate derived geo_point field for distance/sort parity.
        List(s"${f.name}$SpatialCenterSuffix" -> obj("type" -> str("geo_point")))
      case f if f.isInstanceOf[lightdb.field.Field.FacetField[_]] =>
        // Derived keyword tokens to support drill-down + hierarchical facet aggregation.
        // NOTE: do not apply keyword normalization here; facet tokens use reserved markers like "$ROOT$".
        List(s"${f.name}__facet" -> obj("type" -> str("keyword")))
      case f =>
        val mapping = mappingForField(f, config)
        // If we don't have an explicit mapping (empty object), omit the property and let OpenSearch infer it.
        if (mapping.asObj.value.nonEmpty) List(f.name -> mapping) else Nil
    }.flatten
    obj((baseProps ++ joinProps ++ props): _*)
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
    val analyzer = if (field.isInstanceOf[Tokenized[_]]) "standard" else "standard"
    val keyword = if (config.keywordNormalize) {
      obj("type" -> str("keyword"), "normalizer" -> str(KeywordNormalizerName))
    } else {
      obj("type" -> str("keyword"))
    }
    obj(
      "type" -> str("text"),
      "analyzer" -> str(analyzer),
      "fields" -> obj(
        "keyword" -> keyword
      )
    )
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


