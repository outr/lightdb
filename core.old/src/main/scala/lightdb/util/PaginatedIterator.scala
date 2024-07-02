package lightdb.util

import scala.collection.AbstractIterator

case class PaginatedIterator[T](page1: Iterator[T], fetchPage: Int => Option[Iterator[T]]) extends AbstractIterator[T] {
  private var currentPage = 1
  private var currentIterator = page1
  private var nextIterator: Option[Iterator[T]] = None

  override def hasNext: Boolean = {
    if (currentIterator.hasNext) true
    else {
      nextIterator = fetchPage(currentPage + 1)
      nextIterator match {
        case Some(nextIter) =>
          currentPage += 1
          currentIterator = nextIter
          hasNext
        case None => false
      }
    }
  }

  override def next(): T = {
    if (!hasNext) throw new NoSuchElementException("No more elements")
    currentIterator.next()
  }
}