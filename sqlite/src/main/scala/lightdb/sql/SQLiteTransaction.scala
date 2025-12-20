package lightdb.sql

import fabric.rw._
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.query.SQLPart
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.ListExtras
import fabric.io.JsonFormatter
import fabric.{Bool, Json, NumDec, NumInt, Str}
import lightdb.sql.query.SQLQuery

case class SQLiteTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                                state: SQLState[Doc, Model],
                                                                                parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  private def ftsTableName: String = s"${store.name}__fts"

  override protected def tokenizedEqualsPart(fieldName: String, value: String): SQLPart = {
    // Prefer FTS5 if enabled; fall back to LIKE if FTS isn't available.
    if (!SQLiteStore.EnableFTS) return super.tokenizedEqualsPart(fieldName, value)
    val tokens = value.split("\\s+").toList.map(_.trim).filter(_.nonEmpty)
    val query = tokens.map(t => s""""$t"""").mkString(" ")
    // contentless fts table stores _id and tokenized columns
    SQLPart(s"_id IN (SELECT _id FROM $ftsTableName WHERE $ftsTableName MATCH ?)", query.json)
  }

  override protected def tokenizedNotEqualsPart(fieldName: String, value: String): SQLPart = {
    if (!SQLiteStore.EnableFTS) return super.tokenizedNotEqualsPart(fieldName, value)
    val tokens = value.split("\\s+").toList.map(_.trim).filter(_.nonEmpty)
    val query = tokens.map(t => s""""$t"""").mkString(" ")
    SQLPart(s"NOT(_id IN (SELECT _id FROM $ftsTableName WHERE $ftsTableName MATCH ?))", query.json)
  }

  private def mvTable(fieldName: String): String = s"${store.name}__mv__${fieldName}"

  private def jsonToMVValue(json: Json): Option[String] = json match {
    case Str(s, _) => Some(s)
    case NumInt(l, _) => Some(l.toString)
    case NumDec(bd, _) => Some(bd.toString)
    case Bool(b, _) => Some(if (b) "1" else "0")
    case fabric.Null => None
    case other => Some(JsonFormatter.Compact(other))
  }

  override protected def arrayContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if (!SQLiteStore.EnableMultiValueIndexes) return None
    val extracted = values.flatMap(jsonToMVValue)
    if (extracted.isEmpty) return Some(SQLPart.Fragment("1=1"))
    val parts = extracted.map { v =>
      SQLPart(s"EXISTS (SELECT 1 FROM ${mvTable(fieldName)} WHERE owner_id = _id AND value = ?)", v.json)
    }
    Some(SQLQuery(parts.intersperse(SQLPart.Fragment(" AND "))))
  }

  override protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if (!SQLiteStore.EnableMultiValueIndexes) return None
    val extracted = values.flatMap(jsonToMVValue)
    if (extracted.isEmpty) return Some(SQLPart.Fragment("1=1"))
    val inner = extracted.map { v =>
      SQLPart(s"EXISTS (SELECT 1 FROM ${mvTable(fieldName)} WHERE owner_id = _id AND value = ?)", v.json)
    }
    Some(SQLQuery(List(SQLPart.Fragment("NOT("), SQLQuery(inner.intersperse(SQLPart.Fragment(" AND "))), SQLPart.Fragment(")"))))
  }

  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] =
    List(SQLPart(s"DISTANCE(${d.field.name}, ?) AS ${d.field.name}Distance", d.from.json))

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"DISTANCE_LESS_THAN(${f.fieldName}Distance, ?)", f.radius.valueInMeters.json)

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = direction match {
    case SortDirection.Ascending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_ASCENDING")
    case SortDirection.Descending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_DESCENDING")
  }
}
