package lightdb.traversal.pipeline

import rapid.{Stream, Task}

import scala.collection.mutable

/**
 * A typed aggregation pipeline stage: transforms a stream of A into a stream of B.
 *
 * This is inspired by MongoDB's aggregation pipeline, but Scala-typed.
 */
trait Stage[-A, +B] {
  def run(in: Stream[A]): Stream[B]
}

object Stage {
  def map[A, B](f: A => B): Stage[A, B] = (in: Stream[A]) => in.map(f)

  def filter[A](p: A => Boolean): Stage[A, A] = (in: Stream[A]) => in.filter(p)

  /**
   * Unwind / flatten an iterable field.
   *
   * Streaming: does not materialize the input.
   */
  def unwind[A, B](f: A => Iterable[B]): Stage[A, B] =
    (in: Stream[A]) => in.flatMap(a => Stream.emits(f(a).toList))

  /**
   * Skip N rows.
   *
   * Streaming: does not materialize the input.
   */
  def skip[A](n: Int): Stage[A, A] = (in: Stream[A]) => in.drop(n)

  /**
   * Limit to N rows.
   *
   * Streaming: does not materialize the input.
   */
  def limit[A](n: Int): Stage[A, A] = (in: Stream[A]) => in.take(n)

  /**
   * Sort rows by a key (ascending by default).
   *
   * Correctness-first: materializes the input to a list.
   *
   * Prefer `sortByPage` for scalable "sort + paginate" workflows.
   */
  def sortBy[A, K](key: A => K, descending: Boolean = false)(implicit ord: Ordering[K]): Stage[A, A] =
    (in: Stream[A]) => Stream.force(
      in.toList.map { list =>
        val sorted = list.sortBy(key)
        Stream.emits(if descending then sorted.reverse else sorted)
      }
    )

  /**
   * Sort rows by a key and then return only the requested page (offset + limit).
   *
   * Streaming (bounded): consumes the input stream without materializing it, keeping only (offset + limit) items
   * in a heap, then emitting the requested page in sorted order.
   *
   * Notes:
   * - Intended for "sort + paginate" at large scale.
   * - If you need the fully sorted stream, use `sortBy` (materializes).
   */
  def sortByPage[A, K](
                        key: A => K,
                        offset: Int,
                        limit: Int,
                        descending: Boolean = false
                      )(implicit ord: Ordering[K]): Stage[A, A] = (in: Stream[A]) => {
    val off = math.max(0, offset)
    val lim = math.max(0, limit)
    if lim == 0 then Stream.empty
    else Stream.force {
      final case class Item(value: A, k: K, idx: Long)

      def cmp(a: Item, b: Item): Int = {
        val base = ord.compare(a.k, b.k)
        val c = if descending then -base else base
        if c != 0 then c else java.lang.Long.compare(a.idx, b.idx)
      }

      // Worst-first ordering so the head is the item to evict when the heap grows beyond k.
      implicit val worstFirst: Ordering[Item] = new Ordering[Item] {
        override def compare(x: Item, y: Item): Int = cmp(x, y)
      }

      val keep = off + lim
      val pq = mutable.PriorityQueue.empty[Item]
      var i = 0L

      in.evalMap { a =>
        Task {
          val it = Item(a, key(a), i)
          i += 1L
          pq.enqueue(it)
          if pq.size > keep then pq.dequeue()
        }
      }.count.map { _ =>
        val kept = pq.clone().dequeueAll.toVector
        val sorted = kept.sortWith((a, b) => cmp(a, b) < 0).map(_.value)
        Stream.emits(sorted.slice(off, off + lim).toList)
      }
    }
  }

  /**
   * Group rows by key and compute an aggregate per key.
   *
   * Streaming implementation: single pass over the input stream, accumulates into a mutable map.
   * Memory is proportional to number of distinct keys (not number of input rows).
   */
  def groupBy[A, K, S, Out](key: A => K, acc: Accumulator[A, S, Out]): Stage[A, (K, Out)] =
    (in: Stream[A]) => Stream.force {
      val map = mutable.HashMap.empty[K, S]
      in.evalMap { a =>
        Task {
          val k = key(a)
          val s0 = map.getOrElse(k, acc.zero)
          map.update(k, acc.add(s0, a))
        }
      }.count.map { _ =>
        Stream.emits(map.iterator.map { case (k, s) => k -> acc.result(s) }.toList)
      }
    }

  /**
   * Group rows by key and compute two aggregates per key.
   *
   * Streaming implementation: single pass over the input stream, accumulates into a mutable map.
   * Memory is proportional to number of distinct keys (not number of input rows).
   */
  def groupBy2[A, K, S1, O1, S2, O2](
                                      key: A => K,
                                      a1: Accumulator[A, S1, O1],
                                      a2: Accumulator[A, S2, O2]
                                    ): Stage[A, (K, (O1, O2))] =
    (in: Stream[A]) => Stream.force {
      val map = mutable.HashMap.empty[K, (S1, S2)]
      in.evalMap { a =>
        Task {
          val k = key(a)
          val (s1, s2) = map.getOrElse(k, (a1.zero, a2.zero))
          map.update(k, (a1.add(s1, a), a2.add(s2, a)))
        }
      }.count.map { _ =>
        Stream.emits(map.iterator.map { case (k, (s1, s2)) => k -> (a1.result(s1), a2.result(s2)) }.toList)
      }
    }

  /**
   * Group rows by key and compute three aggregates per key.
   *
   * Streaming implementation: single pass over the input stream, accumulates into a mutable map.
   * Memory is proportional to number of distinct keys (not number of input rows).
   */
  def groupBy3[A, K, S1, O1, S2, O2, S3, O3](
                                              key: A => K,
                                              a1: Accumulator[A, S1, O1],
                                              a2: Accumulator[A, S2, O2],
                                              a3: Accumulator[A, S3, O3]
                                            ): Stage[A, (K, (O1, O2, O3))] =
    (in: Stream[A]) => Stream.force {
      val map = mutable.HashMap.empty[K, (S1, S2, S3)]
      in.evalMap { a =>
        Task {
          val k = key(a)
          val (s1, s2, s3) = map.getOrElse(k, (a1.zero, a2.zero, a3.zero))
          map.update(k, (a1.add(s1, a), a2.add(s2, a), a3.add(s3, a)))
        }
      }.count.map { _ =>
        Stream.emits(map.iterator.map { case (k, (s1, s2, s3)) =>
          k -> (a1.result(s1), a2.result(s2), a3.result(s3))
        }.toList)
      }
    }

  /**
   * Facet into two sub-pipelines over the same upstream input.
   *
   * Correctness-first: materializes upstream input once so it can be replayed into both branches.
   *
   * Prefer `reduce2` (and accumulator-based facets like `Accumulators.facetTop`) when both branches are reducers.
   */
  final case class Facet2Result[L, R](left: List[L], right: List[R])

  def facet2[A, L, R](left: Stage[A, L], right: Stage[A, R]): Stage[A, Facet2Result[L, R]] =
    (in: Stream[A]) => Stream.force(
      in.toList.flatMap { list =>
        val base = Stream.emits(list)
        val leftT = left.run(base).toList
        val rightT = right.run(base).toList
        for
          l <- leftT
          r <- rightT
        yield Stream.emit(Facet2Result(l, r))
      }
    )

  /**
   * Facet-like reducer: compute the top-N most common keys.
   *
   * Streaming: single pass over the input. Memory is proportional to the number of distinct keys.
   * Useful for "facets" in pipelines without needing to materialize/replay the upstream like `facet2`.
   *
   * Ordering: primary by descending count, secondary by key ordering (ascending).
   */
  def facetTop[A, K](key: A => K, limit: Int)(implicit ord: Ordering[K]): Stage[A, (K, Long)] =
    (in: Stream[A]) => {
      val lim = math.max(0, limit)
      if lim == 0 then Stream.empty
      else Stream.force {
        val counts = mutable.HashMap.empty[K, Long]
        in.evalMap { a =>
          Task {
            val k = key(a)
            counts.updateWith(k) {
              case Some(c) => Some(c + 1L)
              case None => Some(1L)
            }
          }
        }.count.map { _ =>
          // Maintain a worst-first heap of size lim while iterating the counts map.
          def better(a: (K, Long), b: (K, Long)): Boolean = {
            val c = java.lang.Long.compare(a._2, b._2)
            if c != 0 then c > 0
            else ord.compare(a._1, b._1) < 0
          }

          implicit val worstFirst: Ordering[(K, Long)] = new Ordering[(K, Long)] {
            override def compare(x: (K, Long), y: (K, Long)): Int = {
              if better(x, y) then -1
              else if better(y, x) then 1
              else 0
            }
          }

          val pq = mutable.PriorityQueue.empty[(K, Long)]
          counts.foreach { case kv @ (_, _) =>
            pq.enqueue(kv)
            if pq.size > lim then pq.dequeue()
          }

          val top = pq.clone().dequeueAll.toList.sortWith((a, b) => better(a, b))
          Stream.emits(top)
        }
      }
    }

  /**
   * Reduce the entire input stream to a single output using an accumulator.
   *
   * Streaming: single pass over the input.
   */
  def reduce[A, S, Out](acc: Accumulator[A, S, Out]): Stage[A, Out] =
    (in: Stream[A]) => Stream.force {
      var state = acc.zero
      in.evalMap { a =>
        Task {
          state = acc.add(state, a)
        }
      }.drain.map { _ =>
        Stream.emit(acc.result(state))
      }
    }

  /**
   * Reduce the entire input stream to two outputs in one pass.
   *
   * This is a streaming alternative to `facet2` when both branches are reducers (not full pipelines).
   */
  def reduce2[A, S1, O1, S2, O2](
                                  a1: Accumulator[A, S1, O1],
                                  a2: Accumulator[A, S2, O2]
                                ): Stage[A, (O1, O2)] =
    (in: Stream[A]) => Stream.force {
      var s1 = a1.zero
      var s2 = a2.zero
      in.evalMap { a =>
        Task {
          s1 = a1.add(s1, a)
          s2 = a2.add(s2, a)
        }
      }.drain.map { _ =>
        Stream.emit((a1.result(s1), a2.result(s2)))
      }
    }

  def count[A]: Stage[A, Long] = (in: Stream[A]) => Stream.force(in.count.map(c => Stream.emit(c.toLong)))
}