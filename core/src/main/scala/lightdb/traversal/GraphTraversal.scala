package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.{Stream, Task}

/**
 * Primary entry point for graph traversals
 */
object GraphTraversal {
  /**
   * Start a traversal from a single document ID
   */
  def from[D <: Document[D]](id: Id[D]): DocumentTraversalBuilder[D] =
    new DocumentTraversalBuilder(Stream.emit(id))

  /**
   * Start a traversal from a set of document IDs
   */
  def from[D <: Document[D]](ids: Set[Id[D]]): DocumentTraversalBuilder[D] =
    new DocumentTraversalBuilder(Stream.emits(ids.toSeq))

  /**
   * Start a traversal from a stream of document IDs
   */
  def fromStream[D <: Document[D]](idStream: Stream[Id[D]]): DocumentTraversalBuilder[D] =
    new DocumentTraversalBuilder(idStream)

  // Add this to the GraphTraversal object

  /**
   * Create a traversal using a step function, for compatibility with legacy code
   *
   * @param startIds The set of starting node IDs
   * @param step The step function that returns neighbors for a node
   * @param maxDepth The maximum traversal depth
   * @return A StepFunctionTraversal instance
   */
  def withStepFunction[N <: Document[N]](
                                          startIds: Set[Id[N]],
                                          step: Id[N] => Task[Set[Id[N]]],
                                          maxDepth: Int = Int.MaxValue
                                        ): StepFunctionTraversal[N] = {
    new StepFunctionTraversal[N](startIds, step, maxDepth)
  }

  /**
   * Create a type-safe step function from an edge type
   * This provides a type-safe way to create step functions for tests
   *
   * @param tx The transaction to use
   * @return A type-safe step function
   */
  def createEdgeStepFunction[E <: EdgeDocument[E, N, N], N <: Document[N], M <: DocumentModel[E]](
                                                                                                   tx: PrefixScanningTransaction[E, M]
                                                                                                 ): Id[N] => Task[Set[Id[N]]] = { id =>
    // Use the transaction's traversal methods in a type-safe way
    tx.traversal.edgesFor[E, N, N](id)
      .map(_._to)
      .toList
      .map(_.toSet)
  }

  /**
   * A traversal that uses a step function to find neighbors
   * This provides compatibility with legacy code that used BFSEngine.withStepFunction
   */
  class StepFunctionTraversal[N <: Document[N]](
                                                 startIds: Set[Id[N]],
                                                 step: Id[N] => Task[Set[Id[N]]],
                                                 maxDepth: Int
                                               ) {
    /**
     * Collect all nodes reachable from the starting nodes
     * This mimics the BFSEngine.StepFunctionBFSEngine.collectAllReachable() method
     */
    def collectAllReachable: Task[Set[Id[N]]] = {
      // This replicates the loop function from the original BFSEngine
      def loop(frontier: Set[Id[N]], visited: Set[Id[N]], depth: Int): Task[Set[Id[N]]] = {
        if (frontier.isEmpty || depth > maxDepth) {
          Task.pure(visited)
        } else {
          for {
            lists <- Task.sequence(frontier.toList.map(step))
            next = lists.flatten.toSet -- visited
            out <- loop(next, visited ++ next, depth + 1)
          } yield out
        }
      }

      // Start the traversal with the same logic as the original BFSEngine
      loop(startIds, startIds, depth = 1)
    }

    /**
     * Get a stream of all reachable nodes
     */
    def stream: Stream[Id[N]] = {
      // Track visited nodes to avoid cycles
      val visited = new java.util.concurrent.ConcurrentHashMap[Id[N], Boolean]()
      startIds.foreach(id => visited.put(id, true))

      // Function to process nodes using BFS
      def bfs(frontier: List[Id[N]], depth: Int): Stream[Id[N]] = {
        if (frontier.isEmpty || depth > maxDepth) {
          Stream.empty
        } else {
          // Process current frontier
          Stream.emits(frontier).append {
            // Get next frontier
            Stream.emits(frontier)
              .evalMap(step)
              .flatMap { set =>
                val newNodes = set.filter(id => Option(visited.putIfAbsent(id, true)).isEmpty)
                bfs(newNodes.toList, depth + 1)
              }
          }
        }
      }

      // Start with the startIds as frontier
      Stream.emits(startIds.toList).append(
        bfs(startIds.toList, 1)
      )
    }
  }
}

/**
 * Builder for document traversals
 *
 * @param idStream The stream of document IDs to start the traversal from
 */
class DocumentTraversalBuilder[D <: Document[D]](val idStream: Stream[Id[D]]) {
  private var maxDepth: Int = Int.MaxValue

  /**
   * Configure the maximum traversal depth
   */
  def withMaxDepth(depth: Int): DocumentTraversalBuilder[D] = {
    maxDepth = depth
    this
  }

  /**
   * Follow edges to traverse the graph
   *
   * @tparam E The edge document type
   * @tparam T The target document type
   * @param prefixTx A transaction that supports prefix scanning for the edge type
   * @return A builder for edge traversals
   */
  def follow[E <: EdgeDocument[E, D, T], T <: Document[T]](
                                                            prefixTx: PrefixScanningTransaction[E, _]
                                                          ): EdgeTraversalBuilder[E, D, T] = {
    new EdgeTraversalBuilder(idStream, prefixTx, maxDepth)
  }

  /**
   * Get the stream of document IDs
   */
  def ids: Stream[Id[D]] = idStream

  /**
   * Get the stream of documents
   *
   * @param tx A transaction for retrieving documents
   * @return A stream of documents
   */
  def documents(tx: Transaction[D, _]): Stream[D] = {
    idStream.evalMap(id => tx(id))
  }
}

/**
 * Builder for edge traversals
 *
 * @param fromIds The stream of document IDs to start the traversal from
 * @param tx A transaction that supports prefix scanning for the edge type
 * @param maxDepth The maximum traversal depth
 */
class EdgeTraversalBuilder[E <: EdgeDocument[E, F, T], F <: Document[F], T <: Document[T]](
                                                                                            fromIds: Stream[Id[F]],
                                                                                            tx: PrefixScanningTransaction[E, _],
                                                                                            maxDepth: Int
                                                                                          ) {
  private var edgeFilter: E => Boolean = _ => true
  private var strategy: TraversalStrategy = BFS

  /**
   * Configure edge filtering
   *
   * @param predicate A predicate function to filter edges
   * @return This builder instance
   */
  def filter(predicate: E => Boolean): EdgeTraversalBuilder[E, F, T] = {
    edgeFilter = predicate
    this
  }

  /**
   * Configure traversal strategy
   *
   * @param traversalStrategy The traversal strategy to use
   * @return This builder instance
   */
  def using(traversalStrategy: TraversalStrategy): EdgeTraversalBuilder[E, F, T] = {
    strategy = traversalStrategy
    this
  }

  /**
   * Follow additional edges from the targets of the current traversal
   *
   * @tparam E2 The next edge document type
   * @tparam T2 The next target document type
   * @param nextTx A transaction that supports prefix scanning for the next edge type
   * @return A builder for the next edge traversal
   */
  def follow[E2 <: EdgeDocument[E2, T, T2], T2 <: Document[T2]](nextTx: PrefixScanningTransaction[E2, _]): EdgeTraversalBuilder[E2, T, T2] = {
    val nextStartIds = edges.map(_._to).distinct
    new EdgeTraversalBuilder(nextStartIds, nextTx, maxDepth)
  }

  /**
   * Get the stream of edge documents
   *
   * @return A stream of edge documents
   */
  def edges: Stream[E] = {
    strategy match {
      case BFS => executeBFSEdges()
      case DFS => executeDFSEdges()
    }
  }

  /**
   * Get the stream of target document IDs
   *
   * @return A stream of target document IDs
   */
  def targetIds(implicit ev: F =:= T): Stream[Id[T]] = {
    val startingIds = fromIds.map(_.coerce[T])
    startingIds.append(edges.map(_._to)).distinct
  }

  /**
   * Get the stream of target documents
   *
   * @param docTx A transaction for retrieving target documents
   * @return A stream of target documents
   */
  def documents(docTx: Transaction[T, _]): Stream[T] = {
    edges.map(_._to).distinct.evalMap(id => docTx(id))
  }

  /**
   * Find all paths to a target node
   *
   * @param target The ID of the target node
   * @return A stream of paths to the target node
   */
  def findPaths(target: Id[T]): Stream[TraversalPath[E, F, T]] = {
    implementPathFinding(target, findAll = true)
  }

  /**
   * Find the shortest path to a target node
   *
   * @param target The ID of the target node
   * @return A stream containing the shortest path to the target node, if any
   */
  def findShortestPath(target: Id[T]): Stream[TraversalPath[E, F, T]] = {
    implementPathFinding(target, findAll = false)
  }

  /**
   * Execute a BFS traversal that returns a stream of edges
   */
  private def executeBFSEdges(): Stream[E] = {
    // We'll use a concurrent set to track visited nodes
    val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

    // Function to process a level of the BFS
    def processLevel(frontier: Stream[Id[F]], depth: Int): Stream[E] = {
      if (depth > maxDepth) {
        Stream.empty
      } else {
        // Process the current frontier
        val edgesStream = frontier.flatMap { id =>
          tx.prefixStream(id.value)
            .filter(_._from == id)
            .filter(edgeFilter)
        }

        // Collect target IDs for the next frontier
        edgesStream.flatMap { edge =>
          val targetId = edge._to.asInstanceOf[Id[F]]  // Safe cast for reflexive graphs

          // Emit the edge, and if targetId hasn't been visited, include it in next level
          Stream.emit(edge).append {
            // Use Option to safely handle null - None means the key wasn't in the map
            Option(visited.putIfAbsent(targetId, true)) match {
              case None =>
                // The key wasn't previously in the map (null was returned)
                // We'll create a stream with just this one ID for the next level
                val nextFrontierStream = Stream.emit(targetId)

                // Process the next level recursively - no need for Stream.defer as Stream is already lazy
                processLevel(nextFrontierStream, depth + 1)
              case Some(_) =>
                // The key was already in the map
                Stream.empty // Already visited, so don't include in next level
            }
          }
        }
      }
    }

    // Mark starting nodes as visited and begin traversal with a stream of ids
    fromIds.evalTap { id =>
      Task { visited.put(id, true); () }
    }.flatMap { id =>
      processLevel(Stream.emit(id), 1)
    }
  }

  /**
   * Execute a DFS traversal that returns a stream of edges
   */
  private def executeDFSEdges(): Stream[E] = {
    // We'll use a concurrent set to track visited nodes
    val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

    // Recursive function to perform DFS
    def dfs(id: Id[F], depth: Int): Stream[E] = {
      if (depth > maxDepth) {
        Stream.empty
      } else {
        // Get edges from the current node
        val edgesFromNode = tx.prefixStream(id.value)
          .asInstanceOf[Stream[E]]
          .filter(_._from == id)
          .filter(edgeFilter)

        // For each edge, emit it and then visit its target
        edgesFromNode.flatMap { edge =>
          val targetId = edge._to.asInstanceOf[Id[F]]  // Safe cast for reflexive graphs

          // Emit the edge, then recursively visit the target if not visited
          Stream.emit(edge).append {
            // Use Option to safely handle null - None means the key wasn't in the map
            Option(visited.putIfAbsent(targetId, true)) match {
              case None =>
                // The key wasn't previously in the map (null was returned)
                // Stream is already lazy, so no need for Stream.defer
                dfs(targetId, depth + 1)
              case Some(_) =>
                // The key was already in the map
                Stream.empty
            }
          }
        }
      }
    }

    // Mark starting nodes as visited and begin traversal
    fromIds.evalTap { id =>
      Task { visited.put(id, true); () }
    }.flatMap { id =>
      dfs(id, 1)
    }
  }

  /**
   * Implement path finding (both all paths and shortest path)
   *
   * @param target The target node ID
   * @param findAll Whether to find all paths or just the shortest
   * @return A stream of paths
   */
  private def implementPathFinding(target: Id[T], findAll: Boolean): Stream[TraversalPath[E, F, T]] = {
    // Use a breadth-first approach to find paths
    import scala.collection.mutable

    // Path data includes current node and edges taken to get there
    case class PathData(currentId: Id[F], edges: List[E])

    // We'll use a breadth-first approach but emit paths as they're found
    def findPaths(): Stream[TraversalPath[E, F, T]] = {
      // Set up mutable state for the search
      val queue = mutable.Queue[PathData]()
      val visited = mutable.Set[Id[F]]()
      var shortestLength: Option[Int] = None

      // Initialize queue with starting nodes
      fromIds.toList.sync().foreach { id =>
        queue.enqueue(PathData(id, Nil))
        visited.add(id)
      }

      // Create a stream that produces paths as they're found
      def processQueue(): Stream[TraversalPath[E, F, T]] = {
        if (queue.isEmpty) {
          Stream.empty
        } else {
          // Dequeue the next path to explore
          val PathData(currentId, pathEdges) = queue.dequeue()

          // If we're only finding shortest paths and already have a shorter one, skip
          if (shortestLength.exists(pathEdges.length >= _)) {
            // Continue with next item in queue
            processQueue()
          } else {
            // Get edges from current node
            val outgoingEdges = tx.prefixStream(currentId.value)
              .asInstanceOf[Stream[E]]
              .filter(_._from == currentId)
              .filter(edgeFilter)
              .toList.sync()  // Collect to avoid re-execution

            // Find paths that reach the target
            val completedPaths = outgoingEdges
              .filter(_._to == target)
              .map(edge => new TraversalPath[E, F, T](pathEdges :+ edge))

            // If we found paths to target and only want shortest, update shortestLength
            if (!findAll && completedPaths.nonEmpty) {
              val newLength = completedPaths.head.edges.length
              shortestLength = Some(shortestLength.fold(newLength)(len => math.min(len, newLength)))
            }

            // If not at max depth, enqueue next level paths
            if (pathEdges.length < maxDepth) {
              // Add paths to unexplored nodes
              outgoingEdges.foreach { edge =>
                val targetId = edge._to.asInstanceOf[Id[F]]  // Safe cast for reflexive graphs

                // Only follow path if we haven't visited this node in this path
                // For BFS path finding, we track visited nodes per path
                val pathVisited = pathEdges.exists(_._to == targetId)
                if (!pathVisited && targetId != target) { // Don't explore beyond target
                  queue.enqueue(PathData(targetId, pathEdges :+ edge))
                }
              }
            }

            // Emit completed paths and continue processing queue
            if (completedPaths.isEmpty) {
              processQueue()
            } else {
              Stream.emits(completedPaths).append(
                processQueue()
              )
            }
          }
        }
      }

      processQueue()
    }

    findPaths()
  }
}

/**
 * A path in a traversal
 *
 * @param edges The edges that make up the path
 */
class TraversalPath[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](val edges: List[E]) {
  /**
   * Get the sequence of node IDs in the path
   */
  def nodes: List[Id[_]] = edges match {
    case Nil => Nil
    case _ => edges.head._from :: edges.map(_._to)
  }

  /**
   * Get the length of the path
   */
  def length: Int = edges.length
}

object TraversalPath {
  def apply[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                         edges: List[E]
                                                                                       ): TraversalPath[E, From, To] = new TraversalPath(edges)
}

/**
 * Traversal strategies
 */
sealed trait TraversalStrategy
case object BFS extends TraversalStrategy
case object DFS extends TraversalStrategy