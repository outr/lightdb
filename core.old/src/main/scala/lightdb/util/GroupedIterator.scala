package lightdb.util

/**
 * Convenience Iterator that groups sorted elements together based on the grouper function.
 *
 * Note: this is only useful is the underlying iterator is properly sorted by G.
 */
case class GroupedIterator[T, G](i: Iterator[T], grouper: T => G) extends Iterator[(G, List[T])] {
  private var current: Option[(G, T)] = if (i.hasNext) {
    val t = i.next()
    Some(grouper(t), t)
  } else {
    None
  }

  override def hasNext: Boolean = current.isDefined

  override def next(): (G, List[T]) = current match {
    case Some((group, value)) =>
      current = None
      group -> (value :: i.takeWhile { t =>
        val g = grouper(t)
        if (g != group) {
          current = Some((g, t))
          false
        } else {
          true
        }
      }.toList)
    case None => throw new NoSuchElementException("next on empty iterator")
  }
}