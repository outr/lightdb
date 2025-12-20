package lightdb.h2

import fabric.rw._
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.query.SQLPart
import lightdb.sql.{SQLState, SQLStoreTransaction}
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import fabric.{Bool, Json, NumDec, NumInt, Str}
import fabric.io.JsonFormatter
import lightdb.ListExtras
import lightdb.sql.query.SQLQuery

case class H2Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: H2Store[Doc, Model],
                                                                            state: SQLState[Doc, Model],
                                                                            parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  override protected def concatPrefix: String = "STRING_AGG"

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
    if (!H2Store.EnableMultiValueIndexes) return None
    val extracted = values.flatMap(jsonToMVValue)
    if (extracted.isEmpty) return Some(SQLPart.Fragment("1=1"))
    val parts = extracted.map { v =>
      SQLPart(s"EXISTS (SELECT 1 FROM ${mvTable(fieldName)} WHERE owner_id = _id AND value = ?)", v.json)
    }
    Some(SQLQuery(parts.intersperse(SQLPart.Fragment(" AND "))))
  }

  override protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = {
    if (!H2Store.EnableMultiValueIndexes) return None
    val extracted = values.flatMap(jsonToMVValue)
    if (extracted.isEmpty) return Some(SQLPart.Fragment("1=1"))
    val inner = extracted.map { v =>
      SQLPart(s"EXISTS (SELECT 1 FROM ${mvTable(fieldName)} WHERE owner_id = _id AND value = ?)", v.json)
    }
    Some(SQLQuery(List(SQLPart.Fragment("NOT("), SQLQuery(inner.intersperse(SQLPart.Fragment(" AND "))), SQLPart.Fragment(")"))))
  }

  private def tokenize(value: String): List[String] =
    value.split("\\s+").toList.map(_.trim.toLowerCase).filter(_.nonEmpty)

  private def findFirstTokenizedEquals(filter: Option[Filter[Doc]]): Option[(String, String)] = {
    def loop(f: Filter[Doc]): Option[(String, String)] = f match {
      case e: Filter.Equals[Doc, _] if e.value != null && e.value != None && e.field(store.model).isTokenized =>
        e.getJson(store.model) match {
          case Str(s, _) => Some(e.fieldName -> s)
          case _ => None
        }
      case m: Filter.Multi[Doc] =>
        m.filters.iterator.map(fc => loop(fc.filter)).collectFirst { case Some(v) => v }
      case _ => None
    }
    filter.flatMap(loop)
  }

  override protected def bestMatchPlan(filter: Option[Filter[Doc]], direction: lightdb.SortDirection): Option[BestMatchPlan] = {
    // Heuristic best-match ranking for H2. This is intentionally backend-portable and does not require
    // H2 fulltext extensions. It is not indexed like SQLite FTS5, but provides deterministic scoring.
    val (fieldName, raw) = findFirstTokenizedEquals(filter).getOrElse(return None)
    val tokens = tokenize(raw)
    if (tokens.isEmpty) return None
    val scoreColumn = "__score"
    val qualified = s"LOWER(${store.fqn}.${fieldName})"
    val cond = tokens.map(_ => s"CASE WHEN $qualified LIKE ? THEN 1 ELSE 0 END").mkString(" + ")
    val scoreField = SQLPart(s"($cond) AS $scoreColumn", tokens.map(t => s"%$t%".json): _*)
    val dir = direction match {
      case SortDirection.Descending => "DESC"
      case SortDirection.Ascending => "ASC"
    }
    val sortPart = SQLPart.Fragment(s"$scoreColumn $dir")
    Some(BestMatchPlan(scoreColumn, scoreField, Nil, SQLPart.Fragment("1=1"), sortPart))
  }

  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] = List(
    SQLPart(s"GEO_DISTANCE_JSON(${d.field.name}, ?) AS ${d.field.name}Distance", d.from.json),
    SQLPart(s"GEO_DISTANCE_MIN(${d.field.name}, ?) AS ${d.field.name}DistanceMin", d.from.json)
  )

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"GEO_DISTANCE_MIN(${f.fieldName}, ?) <= ?", f.from.json, f.radius.valueInMeters.json)

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
    SQLPart.Fragment(s"${field.name}DistanceMin $dir")
  }
}
