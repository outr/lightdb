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
    if !SQLiteStore.EnableFTS then None
    else {
      findFirstTokenizedEquals(filter).map { raw =>
        val q = tokenizeQuery(raw)
        val fts = SqlIdent.quote(ftsTableName)
        val idCol = SqlIdent.quote("_id")
        // USING("_id") avoids ambiguous column name errors when selecting `_id` from base+fts.
        val join = List(SQLPart.Fragment(s" JOIN $fts USING($idCol)"))
        val matchFilter = SQLPart(s"$fts MATCH ?", q.json)
        val scoreField = SQLPart.Fragment(s"(-bm25($fts)) AS $scoreColumn")
        val dir = direction match {
          case SortDirection.Descending => "DESC"
          case SortDirection.Ascending => "ASC"
        }
        val sortPart = SQLPart.Fragment(s"$scoreColumn $dir")
        BestMatchPlan(scoreColumn, scoreField, join, matchFilter, sortPart)
      }
    }
  }

  override protected def tokenizedEqualsPart(fieldName: String, value: String): SQLPart = {
    // Prefer FTS5 if enabled; fall back to LIKE if FTS isn't available.
    if !SQLiteStore.EnableFTS then super.tokenizedEqualsPart(fieldName, value)
    else {
      val query = tokenizeQuery(value)
      val idCol = SqlIdent.quote("_id")
      val fts = SqlIdent.quote(ftsTableName)
      // contentless fts table stores _id and tokenized columns
      SQLPart(s"$idCol IN (SELECT $idCol FROM $fts WHERE $fts MATCH ?)", query.json)
    }
  }

  override protected def tokenizedNotEqualsPart(fieldName: String, value: String): SQLPart = {
    if !SQLiteStore.EnableFTS then super.tokenizedNotEqualsPart(fieldName, value)
    else {
      val query = tokenizeQuery(value)
      val idCol = SqlIdent.quote("_id")
      val fts = SqlIdent.quote(ftsTableName)
      SQLPart(s"NOT($idCol IN (SELECT $idCol FROM $fts WHERE $fts MATCH ?))", query.json)
    }
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
