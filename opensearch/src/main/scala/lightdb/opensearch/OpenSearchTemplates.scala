package lightdb.opensearch

import fabric._
import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.Tokenized
import lightdb.opensearch.client.OpenSearchConfig

object OpenSearchTemplates {
  val SpatialCenterSuffix: String = "__center"
  // Stored in _source so we can do prefix queries / sort-by-id without depending on OpenSearch's metadata _id behavior.
  // Also used as an escape hatch for nested "_id" fields (which are problematic in OpenSearch).
  val InternalIdField: String = "__lightdb_id"

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
                                                                   maxResultWindow: Int = 250_000): Json = obj(
    "settings" -> obj(
      "index" -> obj(
        "max_result_window" -> num(maxResultWindow)
      )
    ),
    "mappings" -> obj(
      "dynamic" -> bool(true),
      "properties" -> properties(model, fields, config, storeName)
    )
  )

  private def properties[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                            fields: List[Field[Doc, _]],
                                                                            config: OpenSearchConfig,
                                                                            storeName: String): Json = {
    val baseProps = List(
      InternalIdField -> obj("type" -> str("keyword"))
    )
    val joinProps: List[(String, Json)] = if (config.joinDomain.nonEmpty && config.joinRole.contains("parent") && config.joinChildren.nonEmpty) {
      val children = arr(config.joinChildren.map(str): _*)
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
        List(s"${f.name}__facet" -> obj("type" -> str("keyword")))
      case f =>
        val mapping = mappingForField(f)
        // If we don't have an explicit mapping (empty object), omit the property and let OpenSearch infer it.
        if (mapping.asObj.value.nonEmpty) List(f.name -> mapping) else Nil
    }.flatten
    obj((baseProps ++ joinProps ++ props): _*)
  }

  private def mappingForField[Doc <: Document[Doc]](field: Field[Doc, _]): Json = field.rw.definition match {
    case DefType.Str | DefType.Enum(_, _) =>
      stringMapping(field)
    case DefType.Bool =>
      obj("type" -> str("boolean"))
    case DefType.Int =>
      obj("type" -> str("long"))
    case DefType.Dec =>
      obj("type" -> str("double"))
    case DefType.Opt(d) =>
      mappingForDefType(field, d)
    case DefType.Arr(d) =>
      mappingForDefType(field, d)
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

  private def stringMapping[Doc <: Document[Doc]](field: Field[Doc, _]): Json = {
    val analyzer = if (field.isInstanceOf[Tokenized[_]]) "standard" else "standard"
    obj(
      "type" -> str("text"),
      "analyzer" -> str(analyzer),
      "fields" -> obj(
        "keyword" -> obj("type" -> str("keyword"))
      )
    )
  }

  private def mappingForDefType[Doc <: Document[Doc]](field: Field[Doc, _], d: DefType): Json = d match {
    case DefType.Str | DefType.Enum(_, _) => stringMapping(field)
    case DefType.Bool => obj("type" -> str("boolean"))
    case DefType.Int => obj("type" -> str("long"))
    case DefType.Dec => obj("type" -> str("double"))
    case DefType.Opt(inner) => mappingForDefType(field, inner)
    case DefType.Arr(inner) => mappingForDefType(field, inner)
    case DefType.Obj(_, _) | DefType.Poly(_, _) => obj("type" -> str("object"), "dynamic" -> bool(true))
    case DefType.Json => obj("type" -> str("keyword"))
    case _ => obj()
  }
}


