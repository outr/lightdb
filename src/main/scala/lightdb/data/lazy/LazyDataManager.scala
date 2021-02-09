package lightdb.data.`lazy`

import lightdb.data.DataManager
import test.Field

class LazyDataManager[T] extends DataManager[LazyData[T]] {
  override def fromArray(array: Array[Byte]): LazyData[T] = ???

  override def toArray(value: LazyData[T]): Array[Byte] = ???
}

trait LazyData[T] {
  def data: T
  def apply[F](field: Field[F, T]): F
}