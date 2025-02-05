package lightdb

import fabric.rw._
import rapid.Unique

case class Id[Doc](value: String) extends AnyVal {
  def bytes: Array[Byte] = {
    val b = toString.getBytes("UTF-8")
    assert(b.length <= 128, s"Must be 128 bytes or less, but was ${b.length} ($value)")
    b
  }

  override def toString: String = s"Id($value)"
}

object Id {
  private lazy val _rw: RW[Id[_]] = RW.string(_.value, Id.apply)

  implicit def rw[T]: RW[Id[T]] = _rw.asInstanceOf[RW[Id[T]]]

  def apply[T](value: String = Unique.sync()): Id[T] = new Id[T](value)

  def toString[T](id: Id[T]): String = id.value
}