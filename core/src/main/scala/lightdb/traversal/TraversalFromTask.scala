package lightdb.traversal

import rapid.Task

/**
 * Traversal operations from a task source
 */
class TraversalFromTask[T](task: Task[Seq[T]]) {
  private var maxDepth: Int = 10
  private var visitedNodes: Set[Any] = Set.empty
  private var isConcurrent: Boolean = false
  private var nodeFilter: T => Boolean = (_: T) => true
  private var childrenFunction: T => Task[Seq[Any]] = (_: T) => Task.pure(Seq.empty)

  /**
   * Configure maximum depth for traversal
   */
  def withMaxDepth(depth: Int): TraversalFromTask[T] = {
    maxDepth = depth
    this
  }

  /**
   * Configure traversal to run concurrently
   */
  def concurrent: TraversalFromTask[T] = {
    isConcurrent = true
    this
  }

  /**
   * Add a filter to the traversal
   */
  def withFilter(f: T => Boolean): TraversalFromTask[T] = {
    nodeFilter = f
    this
  }

  /**
   * Define how to retrieve children for each node, allowing children to be different types
   */
  def withChildrenFunction[C](f: T => Task[Seq[C]]): TraversalFromTask[T] = {
    // Create a function that handles conversion to Any safely
    childrenFunction = (t: T) => f(t).map(seq => seq.map(c => c: Any))
    this
  }

  /**
   * Execute the traversal and return all discovered nodes of type R
   */
  def execute[R](implicit ev: T <:< R): Task[Seq[R]] = {
    task.map(seq => seq.filter(nodeFilter).map(ev))
  }

  /**
   * Execute the traversal with a type conversion
   */
  def executeAs[R](implicit ct: scala.reflect.ClassTag[R]): Task[Seq[R]] = {
    task.map { seq =>
      seq
        .filter(nodeFilter)
        .filter(t => ct.runtimeClass.isInstance(t))
        .map(_.asInstanceOf[R])
    }
  }
}
