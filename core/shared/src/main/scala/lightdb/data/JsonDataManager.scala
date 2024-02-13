package lightdb.data

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._

class JsonDataManager[T: RW] extends DataManager[T] {
  override def fromArray(array: Array[Byte]): T = {
    val jsonString = new String(array, "UTF-8")
    JsonParser(jsonString).as[T]
  }

  override def toArray(value: T): Array[Byte] = {
    val v = value.json
    val jsonString = JsonFormatter.Default(v)
    jsonString.getBytes("UTF-8")
  }
}

object JsonDataManager {
  def apply[T: RW](): JsonDataManager[T] = new JsonDataManager[T]
}