package lightdb

sealed trait SortDirection

object SortDirection {
  case object Ascending extends SortDirection

  case object Descending extends SortDirection
}