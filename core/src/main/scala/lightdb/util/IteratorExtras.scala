package lightdb.util

case class IteratorExtras[T](iterator: Iterator[T]) extends AnyVal {
  def head: T = iterator.next()
  def headOption: Option[T] = iterator.nextOption()
  def last: T = lastOption.getOrElse(throw new NullPointerException("Iterator is empty"))
  def lastOption: Option[T] = {
    var o = Option.empty[T]
    iterator.foreach(t => o = Some(t))
    o
  }
}
