package lightdb.traversal

import rapid.{Stream, Task}

/**
 * Traversal operations from a stream source
 */
class TraversalFromStream[T](stream: Stream[T]) {
  private var maxDepth: Int = 10
  private var visitedNodes: Set[Any] = Set.empty
  private var isConcurrent: Boolean = false
  private var nodeFilter: T => Boolean = (_: T) => true
  private var childrenFunction: T => Task[Seq[Any]] = (_: T) => Task.pure(Seq.empty)

  /**
   * Configure maximum depth for traversal
   */
  def withMaxDepth(depth: Int): TraversalFromStream[T] = {
    maxDepth = depth
    this
  }

  /**
   * Configure traversal to run concurrently
   */
  def concurrent: TraversalFromStream[T] = {
    isConcurrent = true
    this
  }

  /**
   * Add a filter to the traversal
   */
  def withFilter(f: T => Boolean): TraversalFromStream[T] = {
    nodeFilter = f
    this
  }

  /**
   * Define how to retrieve children for each node, allowing children to be different types
   */
  def withChildrenFunction[C](f: T => Task[Seq[C]]): TraversalFromStream[T] = {
    // Create a function that handles conversion to Any safely
    childrenFunction = (t: T) => f(t).map(seq => seq.map(c => c: Any))
    this
  }

  /**
   * Execute the traversal and return all discovered nodes of type R
   */
  def execute[R](implicit ev: T <:< R): Task[Seq[R]] = {
    stream.filter(nodeFilter).map(ev).toList
  }

  /**
   * Execute the traversal with a type conversion
   */
  def executeAs[R](implicit ct: scala.reflect.ClassTag[R]): Task[Seq[R]] = {
    stream
      .filter(nodeFilter)
      .filter(t => ct.runtimeClass.isInstance(t))
      .map(_.asInstanceOf[R])
      .toList
  }
}
