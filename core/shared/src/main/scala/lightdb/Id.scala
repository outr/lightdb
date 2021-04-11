package lightdb

import fabric.rw._

class Id[T](val value: String) extends AnyVal {
  def bytes: Array[Byte] = toString.getBytes("UTF-8")

  override def toString: String = value
}

object Id {
  private lazy val _rw: ReaderWriter[Id[_]] = ccRW

  implicit def rw[T]: ReaderWriter[Id[T]] = _rw.asInstanceOf[ReaderWriter[Id[T]]]

  def apply[T](value: String = Unique()): Id[T] = new Id[T](value)

  def toString[T](id: Id[T]): String = id.value

  def fromString[T](s: String): Id[T] = apply[T](s)
}