package lightdb

import lightdb.field.Field

package object query {
  implicit class FieldQueryExtras[D <: Document[D], F](val field: Field[D, F]) extends AnyVal {
    def ===(value: F): Filter[D] = Filter.Equals(field, value)
    def is(value: F): Filter[D] = Filter.Equals(field, value)
    def includes(values: Seq[F]): Filter[D] = Filter.Includes(field, values)
    def excludes(values: Seq[F]): Filter[D] = Filter.Excludes(field, values)
  }
}