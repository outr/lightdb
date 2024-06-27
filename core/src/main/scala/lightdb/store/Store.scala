package lightdb.store

import fabric.rw._
import lightdb.Id
import lightdb.document.SetType

import scala.annotation.tailrec

trait Store[D] {
  implicit val rw: RW[D]

  def internalCounter: Boolean

  def idIterator: Iterator[Id[D]]

  def iterator: Iterator[D]

  def contains(id: Id[D]): Boolean = get(id).nonEmpty

  def get(id: Id[D]): Option[D]

  def put(id: Id[D], doc: D): Option[SetType]

  def delete(id: Id[D]): Boolean

  def count: Int

  def commit(): Unit

  def dispose(): Unit

  def truncate(): Unit = internalTruncate()

  @tailrec
  final def internalTruncate(): Unit = {
    idIterator.foreach(delete)
    if (count > 0) {
      internalTruncate()
    }
  }
}