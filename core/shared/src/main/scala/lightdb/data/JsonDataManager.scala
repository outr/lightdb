package lightdb.data

import fabric.parse._
import fabric.rw._

class JsonDataManager[T: ReaderWriter] extends DataManager[T] {
  override def fromArray(array: Array[Byte]): T = {
    val jsonString = new String(array, "UTF-8")
    Json.parse(jsonString).as[T]
  }

  override def toArray(value: T): Array[Byte] = {
    val v = value.toValue
    val jsonString = Json.format(v)
    jsonString.getBytes("UTF-8")
  }
}

object JsonDataManager {
  def apply[T: ReaderWriter](): JsonDataManager[T] = new JsonDataManager[T]
}