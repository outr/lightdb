package lightdb

import fabric.rw.RW

sealed trait SortDirection

object SortDirection {
  implicit val rw: RW[SortDirection] = RW.enumeration(List(Ascending, Descending))

  case object Ascending extends SortDirection
  case object Descending extends SortDirection
}