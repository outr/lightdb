package lightdb.query

import lightdb.field.Field

sealed trait Filter

object Filter {
  case class Equals[T, F](field: Field[T, F], value: F) extends Filter

  case class NotEquals[T, F](field: Field[T, F], value: F) extends Filter
}