package lightdb.traversal.pipeline

import scala.collection.mutable

/**
 * Streaming-friendly accumulator definition.
 *
 * S is the mutable/immutable state type, Out is the final output type.
 */
trait Accumulator[-A, S, +Out] {
  def zero: S
  def add(state: S, value: A): S
  def result(state: S): Out
}

object Accumulator {
  def count[A]: Accumulator[A, Long, Long] = new Accumulator[A, Long, Long] {
    override def zero: Long = 0L
    override def add(state: Long, value: A): Long = state + 1L
    override def result(state: Long): Long = state
  }

  def sum[A](f: A => Double): Accumulator[A, Double, Double] = new Accumulator[A, Double, Double] {
    override def zero: Double = 0.0
    override def add(state: Double, value: A): Double = state + f(value)
    override def result(state: Double): Double = state
  }

  def min[A, B](f: A => B)(implicit ord: Ordering[B]): Accumulator[A, Option[B], Option[B]] =
    new Accumulator[A, Option[B], Option[B]] {
      override def zero: Option[B] = None
      override def add(state: Option[B], value: A): Option[B] = {
        val b = f(value)
        state match {
          case None => Some(b)
          case Some(prev) => Some(if ord.lteq(b, prev) then b else prev)
        }
      }
      override def result(state: Option[B]): Option[B] = state
    }

  def max[A, B](f: A => B)(implicit ord: Ordering[B]): Accumulator[A, Option[B], Option[B]] =
    new Accumulator[A, Option[B], Option[B]] {
      override def zero: Option[B] = None
      override def add(state: Option[B], value: A): Option[B] = {
        val b = f(value)
        state match {
          case None => Some(b)
          case Some(prev) => Some(if ord.gteq(b, prev) then b else prev)
        }
      }
      override def result(state: Option[B]): Option[B] = state
    }

  /**
   * Facet-like accumulator: compute the top-N most common keys.
   *
   * Ordering: primary by descending count, secondary by key ordering (ascending).
   *
   * Note: this uses a mutable HashMap as its state for performance. `add` mutates and returns the same instance.
   */
  def facetTop[A, K](key: A => K, limit: Int)(implicit ord: Ordering[K]): Accumulator[A, mutable.HashMap[K, Long], List[(K, Long)]] =
    new Accumulator[A, mutable.HashMap[K, Long], List[(K, Long)]] {
      private val lim: Int = math.max(0, limit)

      override def zero: mutable.HashMap[K, Long] = mutable.HashMap.empty[K, Long]

      override def add(state: mutable.HashMap[K, Long], value: A): mutable.HashMap[K, Long] = {
        val k = key(value)
        state.updateWith(k) {
          case Some(c) => Some(c + 1L)
          case None => Some(1L)
        }
        state
      }

      override def result(state: mutable.HashMap[K, Long]): List[(K, Long)] = {
        if lim == 0 then Nil
        else {
          def better(a: (K, Long), b: (K, Long)): Boolean = {
            val c = java.lang.Long.compare(a._2, b._2)
            if c != 0 then c > 0
            else ord.compare(a._1, b._1) < 0
          }

          implicit val worstFirst: Ordering[(K, Long)] = (x: (K, Long), y: (K, Long)) => {
            if better(x, y) then -1
            else if better(y, x) then 1
            else 0
          }

          val pq = mutable.PriorityQueue.empty[(K, Long)]
          state.foreach { case kv @ (_, _) =>
            pq.enqueue(kv)
            if pq.size > lim then pq.dequeue()
          }

          pq.clone().dequeueAll.toList.sortWith((a, b) => better(a, b))
        }
      }
    }
}


