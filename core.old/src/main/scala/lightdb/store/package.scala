package lightdb

package object store {
  implicit class ByteArrayExtras(val bytes: Array[Byte]) extends AnyVal {
    def string: String = new String(bytes, "UTF-8")
  }
}
