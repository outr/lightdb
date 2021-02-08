package lightdb.data

import profig._

class JsonDataManager[T: ReadWriter] extends DataManager[T] {
  override def fromArray(array: Array[Byte]): T = {
    val jsonString = new String(array, "UTF-8")
    JsonUtil.fromJsonString[T](jsonString)
  }

  override def toArray(value: T): Array[Byte] = {
    val jsonString = JsonUtil.toJsonString[T](value)
    jsonString.getBytes("UTF-8")
  }
}
