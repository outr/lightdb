package lightdb.query

import lightdb.Document
import lightdb.field.Field

sealed trait Filter[D <: Document[D]] {
  def matches(document: D): Boolean
}

object Filter {
  case class Equals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter[D] {
    override def matches(document: D): Boolean = field.getter(document) == value
  }
  case class NotEquals[D <: Document[D], F](field: Field[D, F], value: F) extends Filter[D] {
    override def matches(document: D): Boolean = field.getter(document) != value
  }
  case class Includes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter[D] {
    override def matches(document: D): Boolean = values.contains(field.getter(document))
  }
  case class Excludes[D <: Document[D], F](field: Field[D, F], values: Seq[F]) extends Filter[D] {
    override def matches(document: D): Boolean = !values.contains(field.getter(document))
  }
}