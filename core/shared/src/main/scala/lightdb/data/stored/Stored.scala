package lightdb.data.stored

import java.nio.ByteBuffer

case class Stored(`type`: StoredType, bb: ByteBuffer, values: Map[String, StoredValue]) {
  def apply[T](name: String): T = {
    val sv = values(name)
    sv.`type`.read(sv.offset, bb).asInstanceOf[T]
  }
}