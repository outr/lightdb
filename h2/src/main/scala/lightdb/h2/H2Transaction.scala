package lightdb.h2

import fabric.rw.*
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.query.SQLPart
import lightdb.sql.{SQLState, SQLStoreTransaction, SqlIdent, TextWords}
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import fabric.{Bool, Json, NumDec, NumInt, Str}
import fabric.io.JsonFormatter
import lightdb.ListExtras
import lightdb.sql.query.SQLQuery

case class H2Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: H2Store[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override protected def concatPrefix: String = "STRING_AGG"

  private def mvTable(fieldName: String): String = s"${store.name}__mv__${fieldName}"

  private def jsonToMVValue(json: Json): Option[String] = json match {
    case Str(s, _) => Some(s)
    case NumInt(l, _) => Some(l.toString)
    case NumDec(bd, _) => Some(bd.toString)
    case Bool(b, _) => Some(if b then "1" else "0")
    case fabric.Null => None
    case other => Some(JsonFormatter.Compact(other))
  }

  override protected def arrayContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if !H2Store.EnableMultiValueIndexes then None
    else {
      val extracted = values.flatMap(jsonToMVValue)
      if extracted.isEmpty then Some(SQLPart.Fragment("1=1"))
      else {
        val mv = SqlIdent.quote(mvTable(fieldName))
        val ownerCol = SqlIdent.quote("owner_id")
        val valueCol = SqlIdent.quote("value")
        val idCol = SqlIdent.quote("_id")
        val parts = extracted.map { v =>
          SQLPart(s"EXISTS (SELECT 1 FROM $mv WHERE $ownerCol = $idCol AND $valueCol = ?)", v.json)
        }
        Some(SQLQuery(parts.intersperse(SQLPart.Fragment(" AND "))))
      }
    }
  }

  override protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if !H2Store.EnableMultiValueIndexes then None
    else {
      val extracted = values.flatMap(jsonToMVValue)
      if extracted.isEmpty then Some(SQLPart.Fragment("1=1"))
      else {
        val mv = SqlIdent.quote(mvTable(fieldName))
        val ownerCol = SqlIdent.quote("owner_id")
        val valueCol = SqlIdent.quote("value")
        val idCol = SqlIdent.quote("_id")
        val inner = extracted.map { v =>
          SQLPart(s"EXISTS (SELECT 1 FROM $mv WHERE $ownerCol = $idCol AND $valueCol = ?)", v.json)
        }
        Some(SQLQuery(List(SQLPart.Fragment("NOT("), SQLQuery(inner.intersperse(SQLPart.Fragment(" AND "))), SQLPart.Fragment(")"))))
      }
    }
  }

  override protected def wordsText(column: String): Option[String] =
    Some(s"(' ' || REGEXP_REPLACE(LOWER(COALESCE($column, '')), '${TextWords.Separator}', '  ') || ' ')")

  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] = {
    val col = SqlIdent.quote(d.field.name)
    val aliasJson = SqlIdent.quote(s"${d.field.name}Distance")
    val aliasMin = SqlIdent.quote(s"${d.field.name}DistanceMin")
    List(
      SQLPart(s"GEO_DISTANCE_JSON($col, ?) AS $aliasJson", d.from.json),
      SQLPart(s"GEO_DISTANCE_MIN($col, ?) AS $aliasMin", d.from.json)
    )
  }

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"GEO_DISTANCE_MIN(${SqlIdent.quote(f.fieldName)}, ?) <= ?", f.from.json, f.radius.valueInMeters.json)

  override protected def spatialContainsFilter(f: Filter.SpatialContains[Doc]): SQLPart =
    SQLPart(s"GEO_SPATIAL_CONTAINS(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  override protected def spatialIntersectsFilter(f: Filter.SpatialIntersects[Doc]): SQLPart =
    SQLPart(s"GEO_SPATIAL_INTERSECTS(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val dir = if direction == SortDirection.Descending then "DESC" else "ASC"
    SQLPart.Fragment(s"${SqlIdent.quote(s"${field.name}DistanceMin")} $dir")
  }
}
