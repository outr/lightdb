package lightdb.id

import fabric.rw.RW
import rapid.Unique

trait Id[Doc] extends Any with Ordered[Id[Doc]] {
  def value: String
  def bytes: Array[Byte] = {
    val b = value.getBytes("UTF-8")
    assert(b.length <= 128, s"Must be 128 bytes or less, but was ${b.length} ($value)")
    b
  }

  def coerce[T](implicit ev: Doc =:= T): Id[T] = Id[T](value)

  override def compare(that: Id[Doc]): Int = value.compare(that.value)

  override def toString: String = s"${getClass.getSimpleName}($value)"
}

object Id {
  private lazy val _rw: RW[Id[_]] = RW.string(_.value, StringId.apply)

  implicit def rw[T]: RW[Id[T]] = _rw.asInstanceOf[RW[Id[T]]]

  def apply[T](value: String = Unique.sync()): Id[T] = new StringId[T](value)
}