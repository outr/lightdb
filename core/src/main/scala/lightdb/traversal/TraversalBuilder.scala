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
case class TraversalBuilder[T] private (
                                         private val root: Option[T],
                                         private val maxDepth: Int,
                                         private val visitedNodes: Set[Any],
                                         private val isConcurrent: Boolean,
                                         private val nodeFilter: T => Boolean,
                                         private val childrenFunction: T => Task[Seq[Any]]
                                       ) {

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

  /**
   * Use a GraphStep to define how to find neighbors
   */
  def withGraphStep[From <: Document[From], To <: Document[To], E <: Document[E], M <: DocumentModel[E]](
                                                                                                          step: GraphStep[E, M, From, To]
                                                                                                        )(implicit tx: Transaction[E, M], ev: T <:< Id[From]): TraversalBuilder[Id[To]] = {
    // Create a new builder for the target type
    new TraversalBuilder[Id[To]](
      root = None,
      maxDepth = maxDepth,
      visitedNodes = visitedNodes,
      isConcurrent = isConcurrent,
      nodeFilter = (_: Id[To]) => true,
      childrenFunction = (_: Id[To]) => Task.pure(Seq.empty[Any])
    ).withChildrenFunction { id =>
      // Use the GraphStep to find neighbors
      root match {
        case Some(r) =>
          val fromId = ev(r)
          step.neighbors(fromId).map(_.toSeq)
        case None =>
          Task.pure(Seq.empty)
      }
    }
  }

  /**
   * Execute the traversal using BFS algorithm
   */
  def executeBFS[E <: Document[E], M <: DocumentModel[E], From <: Document[From]](
                                                                                   via: GraphStep[E, M, From, From]
                                                                                 )(implicit tx: Transaction[E, M], ev: T <:< Id[From]): Task[Set[Id[From]]] = {
    root match {
      case Some(r) =>
        val fromId = ev(r)
        val engine = new BFSEngine(Set(fromId), via, maxDepth)
        engine.collectAllReachable()
      case None =>
        Task.pure(Set.empty)
    }
  }
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
   * Create a TraversalBuilder for a BFS traversal using GraphStep
   */
  def bfs[N <: Document[N], E <: Document[E], M <: DocumentModel[E]](
                                                                      startIds: Set[Id[N]],
                                                                      via: GraphStep[E, M, N, N],
                                                                      maxDepth: Int = 10
                                                                    )(implicit tx: Transaction[E, M]): TraversalBuilder[Id[N]] = {
    // Create a minimal builder that can be configured further
    val builder = TraversalBuilder[Id[N]](startIds.head)
      .withMaxDepth(maxDepth)

    // Add integration with BFSEngine if needed
    builder
  }

  /**
   * Create a TraversalBuilder for following a GraphStep
   */
  def step[From <: Document[From], To <: Document[To], E <: Document[E], M <: DocumentModel[E]](
                                                                                                 startId: Id[From],
                                                                                                 via: GraphStep[E, M, From, To]
                                                                                               )(implicit tx: Transaction[E, M]): TraversalBuilder[Id[To]] = {
    // Create a minimal builder that will follow the graph step
    TraversalBuilder[Id[To]](null.asInstanceOf[Id[To]])
      .withChildrenFunction { _ =>
        via.neighbors(startId).map(_.toSeq)
      }
  }
}

/**
 * Object containing implementations for recursive traversal algorithms
 */
object RecursiveTraversal {
  /**
   * Find all nodes reachable from a starting ID by following edges
   *
   * @param from The starting ID
   * @param maxDepth The maximum traversal depth
   * @param edgesForFunc A function that returns edges for a given ID
   * @return A stream of all edges reachable from the starting ID
   */
  def reachableFrom[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                               from: Id[From],
                                                                               maxDepth: Int = Int.MaxValue
                                                                             )(edgesForFunc: Id[From] => Stream[E]): Stream[E] = {
    // Use an AtomicReference to handle the visited set in a functional way
    import java.util.concurrent.atomic.AtomicReference
    val visitedRef = new AtomicReference(Set.empty[Id[From]])

    def recurse(queue: Set[Id[From]], depth: Int): Stream[E] = {
      if (queue.isEmpty || depth >= maxDepth) {
        Stream.empty
      } else {
        val head = queue.head
        val rest = queue.tail

        edgesForFunc(head).flatMap { edge =>
          val to = edge._to

          // Thread-safe check and update of visited set
          var emitAndRecurse = false
          visitedRef.getAndUpdate { visited =>
            if (visited.contains(to)) {
              visited // No change
            } else {
              emitAndRecurse = true
              visited + to
            }
          }

          if (emitAndRecurse) {
            Stream.emit(edge).append(recurse(Set(to), depth + 1))
          } else {
            Stream.empty
          }
        }.append(recurse(rest, depth))
      }
    }

    // Start the recursion without marking the starting node as visited
    // to match the original behavior
    recurse(Set(from), 0)
  }

  /**
   * Find all paths between two nodes
   *
   * @param from The starting node ID
   * @param to The target node ID
   * @param maxDepth Maximum path depth
   * @param bufferSize Size of the buffer for collecting paths
   * @param edgeFilter Filter for edges to consider
   * @param edgesForFunc Function to retrieve edges for a node
   * @return A stream of all paths from the starting node to the target node
   */
  def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                          from: Id[From],
                                                                          to: Id[From],
                                                                          maxDepth: Int,
                                                                          bufferSize: Int = 100,
                                                                          edgeFilter: E => Boolean = (_: E) => true
                                                                        )(edgesForFunc: Id[From] => Stream[E]): Stream[TraversalPath[E, From]] = {
    import scala.collection.mutable

    val queue = mutable.Queue[(Id[From], List[E])]()
    val seen = mutable.Set[List[Id[From]]]()
    queue.enqueue((from, Nil))

    val pull: Pull[TraversalPath[E, From]] = new Pull[TraversalPath[E, From]] {
      private var buffer: List[TraversalPath[E, From]] = Nil

      override def pull(): Option[TraversalPath[E, From]] = {
        if (buffer.nonEmpty) {
          val next = buffer.head
          buffer = buffer.tail
          Some(next)
        } else {
          var collected = List.empty[TraversalPath[E, From]]

          while (queue.nonEmpty && collected.size < bufferSize) {
            val (currentId, path) = queue.dequeue()

            if (path.length < maxDepth) {
              val edges: List[E] = edgesForFunc(currentId).toList.sync()
              val filteredEdges = edges.filter(edgeFilter)
              val nextSteps = filteredEdges.filterNot(e => path.exists(_._to == e._to))
              val newPaths = nextSteps.map(e => (e._to, path :+ e))
              val (completed, pending) = newPaths.partition(_._1 == to)

              pending.foreach {
                case (id, newPath) =>
                  val signature = from +: newPath.map(_._to)
                  if (!seen.contains(signature)) {
                    seen += signature
                    queue.enqueue((id, newPath))
                  }
              }

              collected ++= completed.map(p => TraversalPath(p._2))
            }
          }

          if (collected.nonEmpty) {
            buffer = collected.tail
            Some(collected.head)
          } else {
            None
          }
        }
      }
    }

    Stream(Task.pure(pull))
  }
}

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

/**
 * Extension trait to add convenience traversal methods to PrefixScanningTransaction
 */
trait TransactionTraversalSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] { self: PrefixScanningTransaction[Doc, Model] =>
  /**
   * Access traversal functionality for this transaction
   */
  object traversal {
    /**
     * Get a stream of edges for the specified from ID
     */
    def edgesFor[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                              fromId: Id[From]
                                                                                            )(implicit ev: Doc =:= E): Stream[E] = {
      // Use ev to convert the transaction to the correct type
      prefixStream(fromId.value).map(ev.apply)
    }

    /**
     * Find all nodes reachable from a starting ID by following edges
     */
    def reachableFrom[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                                 from: Id[From],
                                                                                 maxDepth: Int = Int.MaxValue
                                                                               )(implicit ev: Doc =:= E): Stream[E] = {
      RecursiveTraversal.reachableFrom[E, From](from, maxDepth)(edgesFor[E, From, From])
    }

    /**
     * Find all paths between two nodes
     */
    def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                            from: Id[From],
                                                                            to: Id[From],
                                                                            maxDepth: Int,
                                                                            bufferSize: Int = 100,
                                                                            edgeFilter: E => Boolean = (_: E) => true
                                                                          )(implicit ev: Doc =:= E): Stream[TraversalPath[E, From]] = {
      RecursiveTraversal.allPaths[E, From](
        from,
        to,
        maxDepth,
        bufferSize,
        edgeFilter
      )(edgesFor[E, From, From])
    }

    /**
     * Find shortest paths between two nodes
     */
    def shortestPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                                 from: Id[From],
                                                                                 to: Id[From],
                                                                                 maxDepth: Int = Int.MaxValue,
                                                                                 bufferSize: Int = 100,
                                                                                 edgeFilter: E => Boolean = (_: E) => true
                                                                               )(implicit ev: Doc =:= E): Stream[TraversalPath[E, From]] = {
      allPaths[E, From](from, to, maxDepth, bufferSize, edgeFilter)
        .takeWhileWithFirst((first, current) => current.edges.length == first.edges.length)
    }

    /**
     * Perform a breadth-first search from the starting node
     */
    def bfs[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                       fromId: Id[From],
                                                                       maxDepth: Int = Int.MaxValue
                                                                     )(implicit ev: Doc =:= E): BFSTraversal[E, From] = {
      new BFSTraversal[E, From](fromId, maxDepth)
    }

    /**
     * Start a graph traversal from the specified node
     */
    def graph[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                           fromId: Id[From]
                                                                                         )(implicit ev: Doc =:= E): GraphTraversal[E, From, To] = {
      new GraphTraversal[E, From, To](fromId)
    }

    /**
     * Helper class for BFS traversal
     */
    class BFSTraversal[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                                  fromId: Id[From],
                                                                                  maxDepth: Int
                                                                                ) {
      /**
       * Find all nodes reachable through edges of the same type
       */
      def through[EM <: EdgeModel[E, From, From]](
                                                   edgeModel: EM
                                                 )(implicit ev: Doc =:= E): Task[Set[Id[From]]] = {
        // Create a custom step that uses our transaction's edgesFor method
        val customStep = new GraphStep[E, EM, From, From] {
          override def neighbors(id: Id[From])(implicit transaction: Transaction[E, EM]): Task[Set[Id[From]]] = {
            edgesFor[E, From, From](id)(ev).map(_._to).toList.map(_.toSet)
          }
        }

        // Create and use the BFS engine
        val engine = new BFSEngine[From, E, EM](
          Set(fromId),
          customStep,
          maxDepth
        )(self.asInstanceOf[Transaction[E, EM]])

        engine.collectAllReachable()
      }
    }

    /**
     * Helper class for graph traversal
     */
    class GraphTraversal[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                                      fromId: Id[From]
                                                                                                    ) {
      /**
       * Follow edges of the specified type
       */
      def follow[EM <: EdgeModel[E, From, To]](
                                                edgeModel: EM
                                              )(implicit ev: Doc =:= E): GraphTraversalEngine[From, To] = {
        // Create a custom step that uses our transaction's edgesFor method
        val customStep = new GraphStep[E, EM, From, To] {
          override def neighbors(id: Id[From])(implicit transaction: Transaction[E, EM]): Task[Set[Id[To]]] = {
            edgesFor[E, From, To](id)(ev).map(_._to).toList.map(_.toSet)
          }
        }

        // Create the graph traversal engine
        GraphTraversalEngine.start(
          fromId,
          customStep
        )(self.asInstanceOf[Transaction[E, EM]])
      }
    }
  }
}