package lightdb.data.stored

import java.nio.ByteBuffer

object StringType extends ValueType[String] {
  override def read(offset: Int, bytes: ByteBuffer): String = {
    val length = bytes.getInt(offset)
    if (length == 4) {
      ""
    } else {
      bytes.getInt
      val array = new Array[Byte](length)

      (0 until length).foreach { i =>
        val b = bytes.get(offset + 4 + i)
        array(i) = b
      }
      new String(array, "UTF-8")
    }
  }

  override def length(offset: Int, bytes: ByteBuffer): Int = bytes.getInt(offset) + 4

  override def write(bytes: ByteBuffer, value: String): Unit = {
    bytes.putInt(value.length)
    if (value.nonEmpty) {
      bytes.put(value.getBytes("UTF-8"))
    }
  }

  override def length(value: String): Int = (value.length + 1) * 4
}