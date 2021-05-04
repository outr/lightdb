package lightdb.query

import lightdb.field.Field

sealed trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByField[T, F](field: Field[T, F], reverse: Boolean = false) extends Sort
}