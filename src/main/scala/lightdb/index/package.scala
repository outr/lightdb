package lightdb

import lightdb.field.Field
import lightdb.index.feature.IndexFeature

package object index {
  implicit class StringFieldExtras[T](field: Field[T, String]) {
    def indexed: Field[T, String] = field.withFeature(IndexFeature.String)
  }
  implicit class IntFieldExtras[T](field: Field[T, Int]) {
    def indexed: Field[T, Int] = field.withFeature(IndexFeature.Int)
  }
}





