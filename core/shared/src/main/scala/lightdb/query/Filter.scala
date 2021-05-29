package lightdb.query

import lightdb.Document
import lightdb.field.Field

sealed trait Filter

object Filter {
  case class Equals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter
  case class NotEquals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter
  case class Includes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter
  case class Excludes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter
}