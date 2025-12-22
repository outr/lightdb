package lightdb.traversal.pipeline

import rapid.Stream

/**
 * A typed aggregation pipeline stage: transforms a stream of A into a stream of B.
 *
 * This is inspired by MongoDB's aggregation pipeline, but Scala-typed.
 */
trait Stage[-A, +B] {
  def run(in: Stream[A]): Stream[B]
}


