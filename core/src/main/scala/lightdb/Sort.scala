package lightdb

trait Sort

object Sort {
  case object BestMatch extends Sort

  case object IndexOrder extends Sort

  case class ByField[Doc, F](field: Field[Doc, F], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByField[Doc, F] = copy(direction = direction)

    def ascending: ByField[Doc, F] = direction(SortDirection.Ascending)

    def asc: ByField[Doc, F] = direction(SortDirection.Ascending)

    def descending: ByField[Doc, F] = direction(SortDirection.Descending)

    def desc: ByField[Doc, F] = direction(SortDirection.Descending)
  }

  case class ByDistance[Doc](field: Field[Doc, GeoPoint],
                             from: GeoPoint) extends Sort
}