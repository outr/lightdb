package lightdb

trait Sort

object Sort {
  case object BestMatch extends Sort

  case object IndexOrder extends Sort

  case class ByIndex[Doc, F](field: Field[Doc, F], direction: SortDirection = SortDirection.Ascending) extends Sort {
    def direction(direction: SortDirection): ByIndex[Doc, F] = copy(direction = direction)

    def ascending: ByIndex[Doc, F] = direction(SortDirection.Ascending)

    def asc: ByIndex[Doc, F] = direction(SortDirection.Ascending)

    def descending: ByIndex[Doc, F] = direction(SortDirection.Descending)

    def desc: ByIndex[Doc, F] = direction(SortDirection.Descending)
  }

  case class ByDistance[Doc](field: Field[Doc, GeoPoint],
                             from: GeoPoint) extends Sort
}