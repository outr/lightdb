package lightdb.data.stored

import java.nio.ByteBuffer

object IntType extends ValueType[Int] {
  override def read(offset: Int, bytes: ByteBuffer): Int = bytes.getInt(offset)

  override def write(bytes: ByteBuffer, value: Int): Unit = bytes.putInt(value)

  override def length(value: Int): Int = 4

  override def length(offset: Int, bytes: ByteBuffer): Int = 4
}