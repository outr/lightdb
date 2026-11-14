package lightdb.util

/**
 * An iterator that runs `onNext` with each answer of `hasNext` and `onClose` once, when the underlying iterator ends
 * or fails. A failure of the underlying iterator is raised, never taken for its end.
 */
case class ActionIterator[T](underlying: Iterator[T],
                             onNext: Boolean => Unit = _ => (),
                             onClose: () => Unit = () => ()) extends Iterator[T] {
  private var closed = false

  private def close(): Unit = if !closed then {
    closed = true
    onClose()
  }

  override def hasNext: Boolean = !closed && {
    val b = try underlying.hasNext catch {
      case t: Throwable =>
        try close() catch { case c: Throwable => t.addSuppressed(c) }
        throw t
    }
    onNext(b)
    if !b then close()
    b
  }

  override def next(): T = underlying.next()
}
