package lightdb.data

trait DataManager[T] {
  def fromArray(array: Array[Byte]): T
  def toArray(value: T): Array[Byte]
}