package object lightdb {
  implicit class ByteArrayExtras(val bytes: Array[Byte]) extends AnyVal {
    def string: String = new String(bytes, "UTF-8")
  }
}