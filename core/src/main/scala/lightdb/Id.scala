package lightdb

import fabric.rw.RW
import lightdb.util.Unique

case class Id[Doc](value: String) extends AnyVal {
  override def toString: String = s"Id($value)"
}

object Id {
  private lazy val _rw: RW[Id[_]] = RW.string(_.value, Id.apply)

  implicit def rw[T]: RW[Id[T]] = _rw.asInstanceOf[RW[Id[T]]]

  def apply[T](value: String = Unique()): Id[T] = new Id[T](value)

  def toString[T](id: Id[T]): String = id.value
}