package lightdb

import fabric._
import fabric.rw._

class Id[T](val value: String) extends AnyVal {
  def bytes: Array[Byte] = toString.getBytes("UTF-8")
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