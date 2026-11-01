package lightdb.sql

import fabric.rw.*
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.filter.Filter
import lightdb.spatial.Geo
import lightdb.sql.SqlIdent
import lightdb.sql.query.SQLPart
import lightdb.sql.query.SQLQuery
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.ListExtras
import fabric.io.JsonFormatter
import fabric.{Bool, Json, NumDec, NumInt, Str}

case class SQLiteTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: SQLStore[Doc, Model],
  state: SQLState[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends SQLStoreTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private def ftsTableName: String = s"${store.name}__fts"
  private def scoreColumn: String = SqlIdent.quote("__score")

  /** An FTS5 query for `words` as consecutive whole words of column `fieldName` (they hold only letters, digits and underscores). */
  private def ftsPhrase(fieldName: String, words: List[String]): String = s"{$fieldName} : \"${words.mkString(" ")}\""

  private def ftsMatch(query: String): SQLPart = {
    val idCol = SqlIdent.quote("_id")
    val fts = SqlIdent.quote(ftsTableName)
    SQLPart(s"$idCol IN (SELECT $idCol FROM $fts WHERE $fts MATCH ?)", query.json)
  }

  /**
   * With FTS5, its own BM25 (`bm25`) over the terms' phrases, joined LEFT so that a row the filter keeps on another clause stays
   * (scoring 0). FTS5 has no per-phrase weight, so a clause's boost is not applied. Without FTS5, the shared ranking over the
   * `lightdb_words` function.
   */
  override protected def bestMatchPlan(filter: Option[Filter[Doc]], direction: lightdb.SortDirection): Option[BestMatchPlan] =
    if !SQLiteStore.EnableFTS then super.bestMatchPlan(filter, direction)
    else {
      val terms = textTerms(filter)
      if terms.isEmpty then None
      else {
        val fts = SqlIdent.quote(ftsTableName)
        val idCol = SqlIdent.quote("_id")
        val ftsId = SqlIdent.quote("__fts_id")
        val ftsScore = SqlIdent.quote("__fts_score")
        val alias = SqlIdent.quote("__bm")
        val query = terms.map(t => ftsPhrase(t.fieldName, t.words)).distinct.mkString(" OR ")
        val join = List(SQLPart(
          s" LEFT JOIN (SELECT $idCol AS $ftsId, -bm25($fts) AS $ftsScore FROM $fts WHERE $fts MATCH ?) AS $alias ON $alias.$ftsId = ${store.fqn}.$idCol",
          query.json
        ))
        val scoreField = SQLPart.Fragment(s"COALESCE($alias.$ftsScore, 0.0) AS $scoreColumn")
        val dir = direction match {
          case SortDirection.Descending => "DESC"
          case SortDirection.Ascending => "ASC"
        }
        Some(BestMatchPlan(scoreColumn, scoreField, join, SQLPart.Fragment("1=1"), SQLPart.Fragment(s"$scoreColumn $dir")))
      }
    }

  override protected def wordsText(column: String): Option[String] = Some(s"lightdb_words($column)")

  override protected def positionSQL(needle: String, haystack: String): String = s"instr($haystack, $needle)"

  override protected def greatestPart(a: SQLPart, b: SQLPart): SQLPart =
    SQLQuery(List(SQLPart.Fragment("MAX("), a, SQLPart.Fragment(", "), b, SQLPart.Fragment(")")))

  override protected def doubleType: String = "REAL"

  override protected def tokenizedExactPart(fieldName: String, token: String): SQLPart =
    if !SQLiteStore.EnableFTS then super.tokenizedExactPart(fieldName, token)
    else {
      val words = TextWords(token)
      if words.isEmpty then SQLPart.Fragment("1=0") else ftsMatch(ftsPhrase(fieldName, words))
    }

  override protected def tokenizedEqualsPart(fieldName: String, value: String): SQLPart =
    if !SQLiteStore.EnableFTS then super.tokenizedEqualsPart(fieldName, value)
    else {
      val tokens = value.split("\\s+").toList.filter(_.nonEmpty).map(TextWords(_))
      if tokens.isEmpty then SQLPart.Fragment("1=1")
      else if tokens.exists(_.isEmpty) then SQLPart.Fragment("1=0")
      else ftsMatch(tokens.map(ftsPhrase(fieldName, _)).mkString(" AND "))
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
    if !SQLiteStore.EnableMultiValueIndexes then None
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
    if !SQLiteStore.EnableMultiValueIndexes then None
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

  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] = {
    val col = SqlIdent.quote(d.field.name)
    val alias = SqlIdent.quote(s"${d.field.name}Distance")
    List(SQLPart(s"DISTANCE($col, ?) AS $alias", d.from.json))
  }

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"DISTANCE_LESS_THAN(${SqlIdent.quote(s"${f.fieldName}Distance")}, ?)", f.radius.valueInMeters.json)

  override protected def spatialContainsFilter(f: Filter.SpatialContains[Doc]): SQLPart =
    SQLPart(s"SPATIAL_CONTAINS(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  override protected def spatialIntersectsFilter(f: Filter.SpatialIntersects[Doc]): SQLPart =
    SQLPart(s"SPATIAL_INTERSECTS(${SqlIdent.quote(f.fieldName)}, ?) = 1", f.geo.toJson)

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val col = SqlIdent.quote(s"${field.name}Distance")
    direction match {
      case SortDirection.Ascending => SQLPart(s"$col COLLATE DISTANCE_SORT_ASCENDING")
      case SortDirection.Descending => SQLPart(s"$col COLLATE DISTANCE_SORT_DESCENDING")
    }
  }
}
