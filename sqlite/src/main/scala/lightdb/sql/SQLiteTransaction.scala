package lightdb.sql

import fabric.rw._
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.query.SQLPart
import lightdb.sql.query.SQLQuery
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.ListExtras
import fabric.io.JsonFormatter
import fabric.{Bool, Json, NumDec, NumInt, Str}

case class SQLiteTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                                state: SQLState[Doc, Model],
                                                                                parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  private def ftsTableName: String = s"${store.name}__fts"
  private def scoreColumn: String = "__score"

  private def tokenizeQuery(value: String): String = {
    val tokens = value.split("\\s+").toList.map(_.trim).filter(_.nonEmpty)
    tokens.map(t => s""""$t"""").mkString(" ")
  }

  private def findFirstTokenizedEquals(filter: Option[Filter[Doc]]): Option[String] = {
    def loop(f: Filter[Doc]): Option[String] = f match {
      case e: Filter.Equals[Doc, _] if e.value != null && e.value != None && e.field(store.model).isTokenized =>
        e.getJson(store.model) match {
          case Str(s, _) => Some(s)
          case _ => None
        }
      case m: Filter.Multi[Doc] =>
        m.filters.iterator.map(fc => loop(fc.filter)).collectFirst {
          case Some(s) => s
        }
      case _ => None
    }
    filter.flatMap(loop)
  }

  override protected def bestMatchPlan(filter: Option[Filter[Doc]], direction: lightdb.SortDirection): Option[BestMatchPlan] = {
    if !SQLiteStore.EnableFTS then return None
    val raw = findFirstTokenizedEquals(filter).getOrElse(return None)
    val q = tokenizeQuery(raw)
    // Use USING(_id) to avoid ambiguous column name errors when selecting "_id" from the base table + fts table.
    val join = List(SQLPart.Fragment(s" JOIN $ftsTableName USING(_id)"))
    val matchFilter = SQLPart(s"$ftsTableName MATCH ?", q.json)
    val scoreField = SQLPart.Fragment(s"(-bm25($ftsTableName)) AS $scoreColumn")
    val dir = direction match {
      case SortDirection.Descending => "DESC"
      case SortDirection.Ascending => "ASC"
    }
    val sortPart = SQLPart.Fragment(s"$scoreColumn $dir")
    Some(BestMatchPlan(scoreColumn, scoreField, join, matchFilter, sortPart))
  }

  override protected def tokenizedEqualsPart(fieldName: String, value: String): SQLPart = {
    // Prefer FTS5 if enabled; fall back to LIKE if FTS isn't available.
    if !SQLiteStore.EnableFTS then return super.tokenizedEqualsPart(fieldName, value)
    val query = tokenizeQuery(value)
    // contentless fts table stores _id and tokenized columns
    SQLPart(s"_id IN (SELECT _id FROM $ftsTableName WHERE $ftsTableName MATCH ?)", query.json)
  }

  override protected def tokenizedNotEqualsPart(fieldName: String, value: String): SQLPart = {
    if !SQLiteStore.EnableFTS then return super.tokenizedNotEqualsPart(fieldName, value)
    val query = tokenizeQuery(value)
    SQLPart(s"NOT(_id IN (SELECT _id FROM $ftsTableName WHERE $ftsTableName MATCH ?))", query.json)
  }

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
    if !SQLiteStore.EnableMultiValueIndexes then return None
    val extracted = values.flatMap(jsonToMVValue)
    if extracted.isEmpty then return Some(SQLPart.Fragment("1=1"))
    val parts = extracted.map { v =>
      SQLPart(s"EXISTS (SELECT 1 FROM ${mvTable(fieldName)} WHERE owner_id = _id AND value = ?)", v.json)
    }
    Some(SQLQuery(parts.intersperse(SQLPart.Fragment(" AND "))))
  }

  override protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if !SQLiteStore.EnableMultiValueIndexes then return None
    val extracted = values.flatMap(jsonToMVValue)
    if extracted.isEmpty then return Some(SQLPart.Fragment("1=1"))
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
