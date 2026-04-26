package lightdb.tantivy

import com.google.protobuf.ByteString
import fabric.*
import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, RW}
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetValue
import lightdb.field.{Field, IndexingState}
import lightdb.field.Field.FacetField
import scantivy.proto as pb

/** Document <-> pb.Document conversions, schema-aware. */
object TantivyDocConvert {

  /** Sentinel appended to hierarchical facet paths so the "this level — no further descent"
   *  bucket is queryable as a leaf. Mirrors Lucene's convention (see `LuceneTransaction`).
   */
  val FacetRootSentinel: String = "$ROOT$"

  def toPb[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    model: Model,
    schemaFields: List[Field[Doc, _]],
    doc: Doc
  ): pb.Document = {
    val state = new IndexingState
    val pbFields = schemaFields.flatMap { f =>
      val json = f.getJson(doc, state)
      val values = jsonToValues(f, json)
      if values.isEmpty then None else Some(pb.FieldValue(name = f.name, values = values))
    }
    pb.Document(fields = pbFields)
  }

  /** Build a Doc from a `pb.Document` returned by the Tantivy search response. */
  def fromPb[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    model: Model,
    pbDoc: pb.Document
  ): Doc = {
    val obj = pbDocToJson(pbDoc, model)
    obj.as[Doc](model.rw)
  }

  /** pb.Document → fabric JSON object, applying field-aware reshaping (e.g. arr-vs-scalar).
   *  Fills in defaults for any model fields that aren't present in the pb document so that the
   *  case class RW can decode without "missing field" errors (common for fields whose stored
   *  value was Null/empty and so we never sent a FieldValue at write time).
   */
  def pbDocToJson[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    pbDoc: pb.Document,
    model: Model
  ): Obj = {
    val byName: Map[String, pb.FieldValue] = pbDoc.fields.map(fv => fv.name -> fv).toMap
    val pairs = model.fields.map { field =>
      val jsonField = isJsonField(field)
      val isFacet = field.isInstanceOf[FacetField[?]]
      val combined: Json = byName.get(field.name) match {
        case Some(fv) =>
          val jsons = fv.values.map { v =>
            val raw = TantivyValue.toJson(v)
            if isFacet then facetStringToJson(raw)
            else if jsonField then raw match {
              case Str(s, _) => JsonParser(s)
              case other => other
            }
            else raw
          }
          // JSON-typed fields are serialized as a single opaque blob (the whole structure), so
          // the Arr wrapping is already part of the parsed JSON. Other multi-valued fields hold
          // separate scalar entries that we DO need to wrap in Arr.
          if jsonField then jsons.headOption.getOrElse(Null)
          else if field.isArr then Arr(jsons.toVector)
          else jsons.headOption.getOrElse(Null)
        case None =>
          // Field not present in stored doc — synthesize a default from the field type.
          if field.isArr then Arr(Vector.empty) else Null
      }
      field.name -> combined
    }
    obj(pairs*)
  }

  /** Tantivy stores facets as path strings like "/a/b/c"; reshape into the FacetValue JSON the
   *  LightDB model expects (`{"path": [...]}`). Strips the trailing `$ROOT$` sentinel that
   *  hierarchical facets append at write time.
   */
  private def facetStringToJson(raw: Json): Json = raw match {
    case Str(s, _) =>
      val steps = s.stripPrefix("/").stripSuffix("/").split('/').toVector
      val cleaned0 = if steps.length == 1 && steps.head.isEmpty then Vector.empty else steps
      val cleaned = if cleaned0.lastOption.contains(FacetRootSentinel) then cleaned0.dropRight(1) else cleaned0
      obj("path" -> Arr(cleaned.map(Str(_))))
    case other => other
  }

  // -- helpers ------------------------------------------------------------------------------

  private def jsonToValues(field: Field[?, ?], json: Json): Seq[pb.Value] = {
    field match {
      case ff: FacetField[?] => facetJsonToValues(json, hierarchical = ff.hierarchical)
      case _ if isJsonField(field) =>
        // JSON-typed fields are stored as opaque serialized strings (see TantivySchema.unwrap).
        if json == Null then Seq.empty
        else Seq(TantivyValue.string(JsonFormatter.Compact(json)))
      case _ => scalarJsonToValues(field, json)
    }
  }

  private def isJsonField(field: Field[?, ?]): Boolean = unwrap(field.rw.definition.defType) match {
    case DefType.Json | _: DefType.Obj | _: DefType.Poly => true
    case _ => false
  }

  @scala.annotation.tailrec
  private def unwrap(d: DefType): DefType = d match {
    case DefType.Opt(inner) => unwrap(inner.defType)
    case DefType.Arr(inner) => unwrap(inner.defType)
    case other => other
  }

  private def scalarJsonToValues(field: Field[?, ?], json: Json): Seq[pb.Value] = json match {
    case Null => Seq.empty
    case Arr(values, _) => values.flatMap(v => scalarJsonToValues(field, v)).toSeq
    case Str(s, _) => Seq(TantivyValue.string(s))
    case NumInt(n, _) => Seq(TantivyValue.long(n))
    case NumDec(n, _) => Seq(TantivyValue.double(n.toDouble))
    case Bool(b, _) => Seq(TantivyValue.bool(b))
    case obj: Obj => Seq(jsonValue(obj))
  }

  private def facetJsonToValues(json: Json, hierarchical: Boolean): Seq[pb.Value] = json match {
    case Null => Seq.empty
    case Arr(values, _) =>
      values.toSeq.flatMap {
        case obj: Obj =>
          val path = obj.value.get("path") match {
            case Some(Arr(steps, _)) => steps.map(s => s.asString).toList
            case _ => Nil
          }
          // For hierarchical facets, append the $ROOT$ sentinel so the "no further descent"
          // bucket is queryable as a leaf (Lucene convention). For non-hierarchical, drop empty
          // paths.
          val materialized: List[String] =
            if hierarchical then path ::: List(FacetRootSentinel)
            else path
          if materialized.isEmpty then None
          else Some(TantivyValue.facet(FacetValue(materialized)))
        case _ => None
      }
    case _ => Seq.empty
  }

  private def jsonValue(json: Json): pb.Value =
    pb.Value(pb.Value.Kind.JsonValue(pb.JsonValue(
      encoded = ByteString.copyFrom(JsonFormatter.Compact(json).getBytes("UTF-8"))
    )))
}
