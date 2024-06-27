package lightdb.store

import fabric.rw._
import lightdb.Id
import lightdb.document.{Document, SetType}
import lightdb.transaction.Transaction

import scala.annotation.tailrec

trait Store[D <: Document[D]] {
  implicit val rw: RW[D]

  def internalCounter: Boolean

  def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]]

  def iterator(implicit transaction: Transaction[D]): Iterator[D]

  def contains(id: Id[D])(implicit transaction: Transaction[D]): Boolean = get(id).nonEmpty

  def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D]

  def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType]

  def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean

  def count(implicit transaction: Transaction[D]): Int

  def commit()(implicit transaction: Transaction[D]): Unit

  def truncate()(implicit transaction: Transaction[D]): Unit = internalTruncate()

  def dispose(): Unit

  @tailrec
  final def internalTruncate()(implicit transaction: Transaction[D]): Unit = {
    idIterator.foreach(delete)
    if (count > 0) {
      internalTruncate()
    }
  }
}