package lightdb.traversal

import rapid.{Stream, Task}

/**
 * Extension trait to add traversal functionality to Stream
 */
object TraversalExtensions {
  /**
   * Extension methods for Stream
   */
  implicit class StreamTraversalOps[T](stream: Stream[T]) {
    /**
     * Begin a traversal from this stream
     */
    def traverse: TraversalFromStream[T] = new TraversalFromStream[T](stream)
  }

  /**
   * Extension methods for Task
   */
  implicit class TaskTraversalOps[T](task: Task[Seq[T]]) {
    /**
     * Begin a traversal from this task
     */
    def traverse: TraversalFromTask[T] = new TraversalFromTask[T](task)
  }
}
