package lightdb.query

import lightdb.document.Document
import lightdb.index.Index
import lightdb.spatial.GeoPoint

trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByIndex[D <: Document[D], F](index: Index[F, D], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByIndex[D, F] = copy(direction = direction)
    def ascending: ByIndex[D, F] = direction(SortDirection.Ascending)
    def asc: ByIndex[D, F] = direction(SortDirection.Ascending)
    def descending: ByIndex[D, F] = direction(SortDirection.Descending)
    def desc: ByIndex[D, F] = direction(SortDirection.Descending)
  }
  case class ByDistance[D <: Document[D]](index: Index[GeoPoint, D],
                                          from: GeoPoint) extends Sort
}