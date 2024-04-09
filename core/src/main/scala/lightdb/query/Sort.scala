package lightdb.query

import lightdb.Document
import lightdb.index.IndexedField

sealed trait Sort

object Sort {
  case object BestMatch extends Sort
  case object IndexOrder extends Sort
  case class ByField[D <: Document[D], F](field: IndexedField[F, D], reverse: Boolean = false) extends Sort
}