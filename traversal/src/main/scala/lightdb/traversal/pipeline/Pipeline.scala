package lightdb.traversal.pipeline

import rapid.Stream

/**
 * Typed pipeline wrapper around a Stream.
 *
 * This is intentionally small for Phase A; later phases add integration with traversal planning,
 * docId streams, and index-backed stages.
 */
final case class Pipeline[+A](stream: Stream[A]) {
  def pipe[B](stage: Stage[A, B]): Pipeline[B] = Pipeline(stage.run(stream))
  def map[B](f: A => B): Pipeline[B] = pipe(Stages.map(f))
  def filter(p: A => Boolean): Pipeline[A] = pipe(Stages.filter(p))
}

object Pipeline {
  def from[A](stream: Stream[A]): Pipeline[A] = Pipeline(stream)
}



