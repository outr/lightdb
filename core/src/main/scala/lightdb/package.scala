import lightdb.util.IteratorExtras

import scala.language.implicitConversions

package object lightdb {
  implicit def iterator2Extra[T](iterator: Iterator[T]): IteratorExtras[T] = IteratorExtras(iterator)
}