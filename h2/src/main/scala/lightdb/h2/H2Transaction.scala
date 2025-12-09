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

case class H2Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: H2Store[Doc, Model],
                                                                            state: SQLState[Doc, Model],
                                                                            parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model] {
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
