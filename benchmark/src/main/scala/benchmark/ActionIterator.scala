package benchmark

case class ActionIterator[T](underlying: Iterator[T],
                             onNext: Boolean => Unit) extends Iterator[T] {
  override def hasNext: Boolean = {
    val b = underlying.hasNext
    onNext(b)
    b
  }

  override def next(): T = underlying.next()
}
