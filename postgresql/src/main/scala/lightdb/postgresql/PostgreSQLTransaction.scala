package lightdb.postgresql

import fabric.*
import fabric.rw.*
import fabric.io.JsonFormatter
import lightdb.{Sort, SortDirection}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.query.SQLPart
import lightdb.sql.{SQLState, SQLStoreTransaction, SqlIdent}
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.vector.VectorMetric

import java.sql.PreparedStatement

case class PostgreSQLTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: PostgreSQLStore[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override protected def regexpPart(name: String, expression: String): SQLPart =
    SQLPart(s"$name ~ ?", expression.json)

  override protected def concatPrefix: String = "STRING_AGG"

  override protected def likePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name ILIKE ?", pattern.json)

  override protected def notLikePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name NOT ILIKE ?", pattern.json)

  // Array membership against the compact-JSON text column. An element is bounded by `[`/`,` on the
  // left and `,`/`]` on the right, so anchoring on those boundaries gives exact matches for any
  // scalar type (no `42` matching `420`) while remaining LIKE-based, so the pg_trgm GIN index applies.
  private def arrayBoundaryPatterns(v: String): List[String] = List(s"%[$v,%", s"%,$v,%", s"%,$v]%", s"%[$v]%")

  override protected def arrayContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] =
    if (values.isEmpty) None
    else Some {
      val q = SqlIdent.quote(fieldName)
      val (clauses, args) = values.map { json =>
        val patterns = arrayBoundaryPatterns(JsonFormatter.Compact(json))
        (s"(${patterns.map(_ => s"$q ILIKE ?").mkString(" OR ")})", patterns.map(_.json))
      }.unzip
      SQLPart(clauses.mkString(" AND "), args.flatten*)
    }

  override protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] =
    if (values.isEmpty) None
    else Some {
      val q = SqlIdent.quote(fieldName)
      val (clauses, args) = values.map { json =>
        val patterns = arrayBoundaryPatterns(JsonFormatter.Compact(json))
        (s"(${patterns.map(_ => s"$q NOT ILIKE ?").mkString(" AND ")})", patterns.map(_.json))
      }.unzip
      SQLPart(clauses.mkString(" AND "), args.flatten*)
    }

  override def populate(ps: PreparedStatement, arg: Json, index: Int): Unit = arg match {
    case Bool(b, _) => ps.setBoolean(index + 1, b)
    case _ => super.populate(ps, arg, index)
  }

  // pgvector columns surface through JDBC as a PGobject whose text value is the vector literal
  // (e.g. "[1,2,3]"). Unwrap it to the String so the generic row→doc path parses it back into a List.
  override protected def obj2Value(obj: Any): Any = obj match {
    case pg: org.postgresql.util.PGobject => pg.getValue
    case _ => super.obj2Value(obj)
  }

  // KNN distance pseudo-column, bound to the query vector and ordered by alias (see sortByVectorDistance).
  override protected def extraFieldsForVectorDistance(sort: Sort.ByVectorDistance[Doc]): List[SQLPart] = {
    val col = SqlIdent.quote(sort.field.name)
    val alias = SqlIdent.quote(s"${sort.field.name}VectorDistance")
    val op = sort.metric match {
      case VectorMetric.Cosine => "<=>"
      case VectorMetric.Euclidean => "<->"
      case VectorMetric.DotProduct => "<#>"
    }
    List(SQLPart(s"$col $op ?::vector AS $alias", sort.vector.json))
  }

  // Spatial distance/relations via PostGIS (functions created in PostgreSQLStore.initConnection when
  // a Geo field is present). The geo column holds the GeoJSON (a single geometry or an array); the
  // `?` bind is the query geometry's GeoJSON.
  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] = {
    val col = SqlIdent.quote(d.field.name)
    val aliasJson = SqlIdent.quote(s"${d.field.name}Distance")
    val aliasMin = SqlIdent.quote(s"${d.field.name}DistanceMin")
    List(
      SQLPart(s"public.ldb_geo_distance_json($col, ?) AS $aliasJson", d.from.json),
      SQLPart(s"public.ldb_geo_distance_min($col, ?) AS $aliasMin", d.from.json)
    )
  }

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"public.ldb_geo_distance_min(${SqlIdent.quote(f.fieldName)}, ?) <= ?", f.from.json, f.radius.valueInMeters.json)

  override protected def spatialContainsFilter(f: Filter.SpatialContains[Doc]): SQLPart =
    SQLPart(s"public.ldb_geo_spatial_contains(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  override protected def spatialIntersectsFilter(f: Filter.SpatialIntersects[Doc]): SQLPart =
    SQLPart(s"public.ldb_geo_spatial_intersects(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  // Order by the per-row minimum distance pseudo-column emitted by extraFieldsForDistance.
  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
    SQLPart.Fragment(s"${SqlIdent.quote(s"${field.name}DistanceMin")} $dir")
  }
}