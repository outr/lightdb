package lightdb

import lightdb.doc.Document
import lightdb.spatial.Geo

trait Sort

object Sort {
  case object BestMatch extends Sort

  case object IndexOrder extends Sort

  case class ByField[Doc <: Document[Doc], F](field: Field[Doc, F], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByField[Doc, F] = copy(direction = direction)

    def ascending: ByField[Doc, F] = direction(SortDirection.Ascending)

    def asc: ByField[Doc, F] = direction(SortDirection.Ascending)

    def descending: ByField[Doc, F] = direction(SortDirection.Descending)

    def desc: ByField[Doc, F] = direction(SortDirection.Descending)
  }

  case class ByDistance[Doc <: Document[Doc]](field: Field[Doc, Option[Geo.Point]],
                             from: Geo.Point,
                             direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByDistance[Doc] = copy(direction = direction)

    def ascending: ByDistance[Doc] = direction(SortDirection.Ascending)

    def asc: ByDistance[Doc] = direction(SortDirection.Ascending)

    def descending: ByDistance[Doc] = direction(SortDirection.Descending)

    def desc: ByDistance[Doc] = direction(SortDirection.Descending)
  }
}