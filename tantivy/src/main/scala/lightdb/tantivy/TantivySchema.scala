package lightdb.tantivy

import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.{FacetField, Indexed, Tokenized, UniqueIndex}
import lightdb.spatial.Geo
import lightdb.store.StoreMode
import scantivy.proto as pb

/** Compiles a LightDB `DocumentModel` into a scantivy `SchemaDef` plus the chosen primary
 *  `id_field`.
 */
object TantivySchema {
  /** Result of schema mapping. */
  final case class Built(schema: pb.SchemaDef, idField: String)

  def build[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    model: Model,
    storeMode: StoreMode[Doc, Model]
  ): Built = {
    val storeAll = storeMode.isAll
    val fields = model.fields
    val mapped: List[pb.FieldDef] = fields.map(field => mapField(field, storeAll))
    Built(pb.SchemaDef(fields = mapped), idField = "_id")
  }

  private def mapField[Doc <: Document[Doc]](
    field: Field[Doc, _],
    storeAll: Boolean
  ): pb.FieldDef = field match {
    case f: FacetField[Doc] =>
      // Tantivy facet/text fields are intrinsically multi-valued (you just call `add_*`
      // multiple times for the same field), so Scantivy 1.0 dropped the `multiValued` flag.
      pb.FieldDef(
        name = f.name,
        kind = pb.FieldKind.FACET,
        stored = true,
        indexed = true,
        fast = false,
        tokenized = false,
        analyzer = None,
        facetOptions = Some(pb.FacetOptions(
          hierarchical = f.hierarchical,
          requireDimCount = f.requireDimCount
        ))
      )
    case _ if field.isSpatial =>
      throw new UnsupportedOperationException(
        s"Tantivy backend has no native geo/spatial support. Field '${field.name}' has spatial type — pick a different backend (e.g. lucene) or remove the field. (We deliberately do NOT emulate spatial via in-memory scans because it would mislead callers about real-world performance.)"
      )
    case _ =>
      val (kind, fast) = baseKind(field)
      val isTokenized = field match {
        case _: Tokenized[Doc] => true
        case _ => false
      }
      val (analyzer, indexedTokenized) =
        if isTokenized then (Some("default"), true)
        else (None, false)
      // For STORED-mode all stores everything; for INDEXES mode only indexed fields are stored.
      val stored = storeAll || field.indexed
      pb.FieldDef(
        name = field.name,
        kind = kind,
        stored = stored,
        indexed = field.indexed,
        fast = fast,
        tokenized = indexedTokenized,
        analyzer = analyzer,
        facetOptions = None
      )
  }

  /** Map the LightDB field's underlying type → Tantivy FieldKind, plus a sensible `fast` flag.
   *
   *  Fast fields are required for `Sort.ByField` and aggregations; cheap for scalar types. We
   *  enable `fast=true` on any scalar-typed field whose Tantivy kind supports it, regardless of
   *  whether the LightDB field is indexed — non-indexed fields can still be sorted/aggregated.
   */
  private def baseKind[Doc <: Document[Doc]](field: Field[Doc, _]): (pb.FieldKind, Boolean) = {
    val (kind, fastDefault) = unwrap(field.rw.definition.defType, field.indexed)
    (kind, fastDefault)
  }

  private def unwrap(d: DefType, indexed: Boolean): (pb.FieldKind, Boolean) = d match {
    case DefType.Opt(inner) => unwrap(inner.defType, indexed)
    case DefType.Arr(inner) => unwrap(inner.defType, indexed)
    case DefType.Str       => (pb.FieldKind.STRING, true)
    case DefType.Int       => (pb.FieldKind.I64, true)
    case DefType.Dec       => (pb.FieldKind.F64, true)
    case DefType.Bool      => (pb.FieldKind.BOOL, true)
    // Store JSON-typed fields as opaque STRING blobs. Tantivy's native JSON_FIELD requires an
    // object root and supports JSON-path queries we don't expose via LightDB; opaque strings
    // round-trip cleanly for any JSON shape (primitive, array, object).
    case DefType.Json | DefType.Obj(_) | DefType.Poly(_, _) =>
      (pb.FieldKind.STRING, false)
    case DefType.Null      => (pb.FieldKind.STRING, false)
    case _                 => (pb.FieldKind.STRING, false)
  }
}
