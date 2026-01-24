package lightdb.util

import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListSet
import scala.jdk.CollectionConverters.*

class AtomicList[T](comparator: Option[Comparator[T]]) {
  private val set = new ConcurrentSkipListSet[T](comparator.orNull)

  def add(value: T): Boolean = set.add(value)

  def remove(value: T): Boolean = set.remove(value)

  def clear(): Unit = set.clear()

  def iterator: Iterator[T] = set.iterator().asScala

  def reverseIterator: Iterator[T] = set.descendingIterator().asScala
}
