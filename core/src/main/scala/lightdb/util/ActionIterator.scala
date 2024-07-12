package lightdb.util

import scala.util.Try

case class ActionIterator[T](underlying: Iterator[T],
                             onNext: Boolean => Unit = _ => (),
                             onClose: () => Unit = () => ()) extends Iterator[T] {
  private var closed = false

  override def hasNext: Boolean = {
    val b = Try(underlying.hasNext).getOrElse(false)
    onNext(b)
    if (!b && !closed) {
      closed = true
      onClose()
    }
    b
  }

  override def next(): T = underlying.next()
}
