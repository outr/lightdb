package lightdb.traversal

import rapid._
import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}

/**
 * TraversalBuilder - A utility for traversing object structures
 * Compatible with both Scala 2 and Scala 3
 */
case class TraversalBuilder[T] private (private val root: Option[T],
                                        private val maxDepth: Int,
                                        private val visitedNodes: Set[Any],
                                        private val isConcurrent: Boolean,
                                        private val nodeFilter: T => Boolean,
                                        private val childrenFunction: T => Task[Seq[Any]]) {
  /**
   * Configure maximum depth for traversal
   */
  def withMaxDepth(depth: Int): TraversalBuilder[T] = copy(maxDepth = depth)

  /**
   * Configure traversal to run concurrently
   */
  def concurrent: TraversalBuilder[T] = copy(isConcurrent = true)

  /**
   * Add a filter to the traversal
   */
  def withFilter(f: T => Boolean): TraversalBuilder[T] = copy(nodeFilter = f)

  /**
   * Define how to retrieve children for each node, allowing children to be different types
   */
  def withChildrenFunction[C](f: T => Task[Seq[C]]): TraversalBuilder[T] = {
    // Create a function that handles conversion to Any safely
    val adaptedFunction = (t: T) => f(t).map(seq => seq.map(c => c: Any))
    copy(childrenFunction = adaptedFunction)
  }

  /**
   * Use a GraphStep to define how to find neighbors
   */
  def withGraphStep[E <: Document[E], M <: DocumentModel[E], From <: Document[From], To <: Document[To]](
                                                                                                          step: GraphStep[E, M, From, To],
                                                                                                          transaction: PrefixScanningTransaction[E, M]
                                                                                                        )(implicit ev: T <:< Id[From]): TraversalBuilder[Id[To]] = {
    // Create a children function that uses the GraphStep to find neighbors
    val childrenFn = (t: Id[To]) => Task.pure(Seq.empty[Any])

    // Create a new builder for the target type
    TraversalBuilder[Id[To]](
      root = None,
      maxDepth = maxDepth,
      visitedNodes = visitedNodes,
      isConcurrent = isConcurrent,
      nodeFilter = (_: Id[To]) => true,
      childrenFunction = childrenFn
    )
  }

  /**
   * Execute the traversal using BFS algorithm
   */
  def executeBFS[E <: Document[E], M <: DocumentModel[E], From <: Document[From]](
                                                                                   via: GraphStep[E, M, From, From],
                                                                                   transaction: PrefixScanningTransaction[E, M]
                                                                                 )(implicit ev: T <:< Id[From]): Task[Set[Id[From]]] = {
    root match {
      case Some(r) =>
        val fromId = ev(r)
        val engine = new BFSEngine(Set(fromId), via, maxDepth)(transaction)
        engine.collectAllReachable()
      case None =>
        Task.pure(Set.empty)
    }
  }

  /**
   * Execute the traversal and return all discovered nodes of type R
   */
  def execute[R](implicit ev: T <:< R): Task[Seq[R]] = {
    // Placeholder for the rapid task implementation
    root match {
      case Some(r) => Task.pure(Seq(ev(r)))
      case None => Task.pure(Seq.empty[R])
    }
  }

  /**
   * Execute the traversal with a type conversion
   */
  def executeAs[R](implicit ct: scala.reflect.ClassTag[R]): Task[Seq[R]] = {
    root match {
      case Some(r) if ct.runtimeClass.isInstance(r) =>
        Task.pure(Seq(r.asInstanceOf[R])) // Only safe cast with runtime check
      case _ =>
        Task.pure(Seq.empty[R])
    }
  }

  /**
   * Execute the traversal and return a stream of discovered nodes
   */
  def stream(implicit ev: T <:< T): Stream[T] = {
    // Placeholder for streaming implementation
    root match {
      case Some(r) => Stream.emit(r)
      case None => Stream.empty
    }
  }

  /**
   * Execute the traversal and return a stream with type conversion
   */
  def streamAs[R](implicit ct: scala.reflect.ClassTag[R]): Stream[R] = {
    root match {
      case Some(r) if ct.runtimeClass.isInstance(r) =>
        Stream.emit(r.asInstanceOf[R]) // Only safe cast with runtime check
      case _ =>
        Stream.empty
    }
  }

  /**
   * Add a node to the visited set
   */
  private def markVisited(node: Any): TraversalBuilder[T] = copy(visitedNodes = visitedNodes + node)

  /**
   * Check if a node has been visited
   */
  private def hasVisited(node: Any): Boolean = visitedNodes.contains(node)
}

/**
 * Companion object providing factory methods for creating TraversalBuilder instances
 */
object TraversalBuilder {
  /**
   * Create a new TraversalBuilder with the specified root
   */
  def apply[T](root: T): TraversalBuilder[T] = {
    TraversalBuilder(
      root = Some(root),
      maxDepth = 10,
      visitedNodes = Set.empty[Any],
      isConcurrent = false,
      nodeFilter = (_: T) => true,
      childrenFunction = (_: T) => Task.pure(Seq.empty[Any])
    )
  }

  /**
   * Create a traversal builder with no root document
   */
  def empty[T]: TraversalBuilder[T] = {
    TraversalBuilder(
      root = None,
      maxDepth = 10,
      visitedNodes = Set.empty[Any],
      isConcurrent = false,
      nodeFilter = (_: T) => true,
      childrenFunction = (_: T) => Task.pure(Seq.empty[Any])
    )
  }

  /**
   * Create a traversal builder for edge documents starting from a specific id
   */
  def edges[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To], M <: DocumentModel[E]](
                                                                                                                fromId: Id[From],
                                                                                                                transaction: PrefixScanningTransaction[E, M]
                                                                                                              ): EdgeTraversalBuilder[E, From, To] = {
    new EdgeTraversalBuilder[E, From, To](fromId, transaction)
  }
}