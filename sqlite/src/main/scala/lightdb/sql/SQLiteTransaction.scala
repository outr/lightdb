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

case class SQLiteTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                                state: SQLState[Doc, Model],
                                                                                parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] =
    List(SQLPart(s"DISTANCE(${d.field.name}, ?) AS ${d.field.name}Distance", d.from.json))

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"DISTANCE_LESS_THAN(${f.fieldName}Distance, ?)", f.radius.valueInMeters.json)

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = direction match {
    case SortDirection.Ascending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_ASCENDING")
    case SortDirection.Descending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_DESCENDING")
  }
}
