package lightdb

import lightdb.field.{Field, FieldFeature}

package object index {
  implicit class StringFieldExtras[T](field: Field[T, String]) {
    def indexed: Field[T, String] = field.withFeature(StringIndexed)
  }
  implicit class IntFieldExtras[T](field: Field[T, Int]) {
    def indexed: Field[T, Int] = field.withFeature(IntIndexed)
  }
}

sealed trait IndexFeature extends FieldFeature

object StringIndexed extends IndexFeature

object IntIndexed extends IndexFeature