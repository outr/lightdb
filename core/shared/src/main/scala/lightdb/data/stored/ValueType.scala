package lightdb.data.stored

import java.nio.ByteBuffer

trait ValueType[V] {
  def read(offset: Int, bytes: ByteBuffer): V

  def write(bytes: ByteBuffer, value: V): Unit

  def length(value: V): Int

  def length(offset: Int, bytes: ByteBuffer): Int
}