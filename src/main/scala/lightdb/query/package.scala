package lightdb

import lightdb.field.Field

package object query {
  implicit class FieldQueryExtras[D <: Document[D], F](val field: Field[D, F]) extends AnyVal {
    def ===(value: F): Filter = Filter.Equals(field, value)
  }
}