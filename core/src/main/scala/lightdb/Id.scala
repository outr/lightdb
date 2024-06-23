package lightdb

import fabric.rw._
import lightdb.util.Unique

class Id[T](val value: String) {
  private var _persisted: Boolean = false

  /**
   * Determines if the document has been persisted to the database.
   */
  def persisted: Boolean = _persisted

  def bytes: Array[Byte] = {
    val b = toString.getBytes("UTF-8")
    assert(b.length <= 128, s"Must be 128 bytes or less, but was ${b.length} ($value)")
    b
  }

  override def hashCode(): Int = value.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case that: Id[_] => this.value == that.value
    case _ => false
  }

  override def toString: String = value
}

object Id {
  private lazy val _rw: RW[Id[_]] = RW.string(_.value, Id.apply)

  implicit def rw[T]: RW[Id[T]] = _rw.asInstanceOf[RW[Id[T]]]

  def apply[T](value: String = Unique()): Id[T] = new Id[T](value)

  def toString[T](id: Id[T]): String = id.value

  def fromString[T](s: String): Id[T] = apply[T](s)

  private[lightdb] def setPersisted(id: Id[_], persisted: Boolean): Unit = id._persisted = persisted
}