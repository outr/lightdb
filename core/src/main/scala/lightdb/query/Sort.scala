package lightdb.query

import lightdb.Document
import lightdb.index.IndexedField
import lightdb.spatial.GeoPoint

trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByField[D <: Document[D], F](field: IndexedField[F, D], reverse: Boolean = false) extends Sort
  case class ByDistance[D <: Document[D]](field: IndexedField[GeoPoint, D],
                                          from: GeoPoint) extends Sort
}