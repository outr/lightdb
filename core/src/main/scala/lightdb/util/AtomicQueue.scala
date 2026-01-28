package lightdb.util

import lightdb.store.write.WriteOp

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.VectorMap
import scala.collection.mutable.ListBuffer
import scala.compiletime.uninitialized

class AtomicQueue[T <: AnyRef] extends Iterable[T] {
  private val q = new ConcurrentLinkedQueue[T]
  private val _size = new AtomicInteger(0)

  def poll(): Option[T] = {
    val t = q.poll()
    if t == null then None
    else {
      _size.decrementAndGet()
      Some(t)
    }
  }

  def poll(max: Int): ArraySeq[T] = {
    val a = new Array[AnyRef](max)

    @tailrec
    def recurse(count: Int): Int = if (count >= max) {
      // Finished
      count
    } else {
      poll() match {
        case None => count
        case Some(t) =>
          a(count) = t.asInstanceOf[AnyRef]
          recurse(count + 1)
      }
    }
    val n = recurse(0)

    val array = if (n == 0) {
      Array.empty[AnyRef]
    } else if (n == max) {
      a
    } else {
      util.Arrays.copyOf(a, n)
    }
    ArraySeq.unsafeWrapArray(array).asInstanceOf[ArraySeq[T]]
  }

  def add(t: T): Int = {
    q.offer(t)
    _size.incrementAndGet()
  }

  def clear(): Unit = {
    q.clear()
    _size.set(0)
  }

  override def size: Int = _size.get()

  override def iterator: Iterator[T] = new Iterator[T] {
    private var cached: T = uninitialized
    private var cachedValid = false

    private def ensureCached(): Unit =
      if !cachedValid then
        cached = q.poll()
        if cached != null then _size.decrementAndGet()
        cachedValid = true

    override def hasNext: Boolean =
      ensureCached()
      cached != null

    override def next(): T =
      ensureCached()
      val v = cached
      if v == null then throw new NoSuchElementException("next on empty iterator")
      cachedValid = false
      v
  }
}