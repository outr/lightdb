package lightdb.query

import lightdb.document.Document
import lightdb.index.Index
import lightdb.spatial.GeoPoint

trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByField[D <: Document[D], F](field: Index[F, D], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByField[D, F] = copy(direction = direction)
    def ascending: ByField[D, F] = direction(SortDirection.Ascending)
    def asc: ByField[D, F] = direction(SortDirection.Ascending)
    def descending: ByField[D, F] = direction(SortDirection.Descending)
    def desc: ByField[D, F] = direction(SortDirection.Descending)
  }
  case class ByDistance[D <: Document[D]](field: Index[GeoPoint, D],
                                          from: GeoPoint) extends Sort
}