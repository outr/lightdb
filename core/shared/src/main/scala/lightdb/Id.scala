package lightdb

import fabric.rw._

class Id[T](val value: String) extends AnyVal {
  def bytes: Array[Byte] = {
    val b = toString.getBytes("UTF-8")
    assert(b.length <= 128, s"Must be 128 bytes or less, but was ${b.length} ($value)")
    b
  }
  def parts: Vector[String] = value.split('/').toVector

  override def toString: String = value
}

object Id {
  private lazy val _rw: RW[Id[_]] = RW.string(_.value, Id.apply)

  implicit def rw[T]: RW[Id[T]] = _rw.asInstanceOf[RW[Id[T]]]

  def apply[T](value: String = Unique()): Id[T] = new Id[T](value)

  def toString[T](id: Id[T]): String = id.value

  def fromString[T](s: String): Id[T] = apply[T](s)
}