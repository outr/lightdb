package lightdb.util

import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListSet

class AtomicList[T <: Comparable[T]](comparator: Option[Comparator[T]]) {
  private val set = new ConcurrentSkipListSet[T](comparator.orNull)

  def add(value: T): Boolean = set.add(value)

  def remove(value: T): Boolean = set.remove(value)

  def iterator: Iterator[T] = set.iterator().asScala

  def reverseIterator: Iterator[T] = set.descendingIterator().asScala
}
