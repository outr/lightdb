package lightdb.query

import lightdb.Document
import lightdb.field.Field

sealed trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByField[D <: Document[D], F](field: Field[D, F], reverse: Boolean = false) extends Sort
}