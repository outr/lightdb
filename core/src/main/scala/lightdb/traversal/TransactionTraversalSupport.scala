package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.Stream

/**
 * Extension methods for transactions to support traversal functionality
 */
trait TransactionTraversalSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  self: PrefixScanningTransaction[Doc, Model] =>

  /**
   * Provides traversal functionality
   */
  object traversal {
    /**
     * Get a stream of edges for the specified from ID
     */
    def edgesFor[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](fromId: Id[From])
                                                                                            (implicit ev: Doc =:= E): Stream[E] =
      prefixStream(fromId.value).map[E](doc => ev(doc))

    /**
     * Start a traversal from a single document ID
     */
    def from[D <: Document[D]](id: Id[D]): DocumentTraversalBuilder[D] = GraphTraversal.from(id)

    /**
     * Start a traversal from a set of document IDs
     */
    def from[D <: Document[D]](ids: Set[Id[D]]): DocumentTraversalBuilder[D] = GraphTraversal.from(ids)

    /**
     * Find all nodes reachable from a starting ID by following edges
     */
    def reachableFrom[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                                  maxDepth: Int = Int.MaxValue)
                                                                                                 (implicit ev: From =:= To): Stream[E] = {
      // Use the new traversal API internally
      GraphTraversal.from(from)
        .withMaxDepth(maxDepth)
        .follow[E, To](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .edges
    }

    /**
     * Find all paths between two nodes
     */
    def allPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                             to: Id[To],
                                                                                             maxDepth: Int,
                                                                                             bufferSize: Int = 100,
                                                                                             edgeFilter: E => Boolean = (_: E) => true)
                                                                                            (implicit ev: Doc =:= E): Stream[TraversalPath[E, From, To]] = {
      // Use the new traversal API internally
      GraphTraversal.from(from)
        .withMaxDepth(maxDepth)
        .follow[E, To](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .filter(edgeFilter)
        .findPaths(to)
    }

    /**
     * Find shortest paths between two nodes using a direct implementation rather than
     * going through EdgeTraversalBuilder to avoid potential streaming issues
     */
    def shortestPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                                  to: Id[To],
                                                                                                  maxDepth: Int = Int.MaxValue,
                                                                                                  bufferSize: Int = 100,
                                                                                                  edgeFilter: E => Boolean = (_: E) => true)
                                                                                                 (implicit ev: Doc =:= E): Stream[TraversalPath[E, From, To]] = {
      // Direct implementation for shortestPaths - materialized result
      import scala.collection.mutable

      // For shortest paths, we use a level-based BFS to find all paths of the same length
      val result = mutable.ListBuffer.empty[TraversalPath[E, From, To]]

      // Each node can be reached via multiple paths, but we need to ensure
      // we don't create cycles within a single path
      case class PathState(node: Id[From], path: List[E], visited: Set[String])

      // Queue for BFS
      val queue = mutable.Queue.empty[PathState]

      // Track which level we're currently processing
      var currentLevel = 0
      var nodesAtCurrentLevel = 1
      var nodesAtNextLevel = 0
      var foundTarget = false

      // Start with the initial node
      queue.enqueue(PathState(from, Nil, Set(from.value)))

      // BFS loop
      while (queue.nonEmpty && currentLevel <= maxDepth && (!foundTarget || nodesAtCurrentLevel > 0)) {
        val state = queue.dequeue()
        nodesAtCurrentLevel -= 1

        // Check if we've reached the target
        if (state.node.asInstanceOf[Id[To]] == to) {
          result += new TraversalPath[E, From, To](state.path)
          foundTarget = true
        }
        // Only explore further if we haven't found a target yet
        // or if we're still processing the current level where we found it
        else if (currentLevel < maxDepth && !foundTarget) {
          // Get outgoing edges
          prefixStream(state.node.value)
            .map[E](doc => ev(doc))
            .filter(_._from == state.node)
            .filter(edgeFilter)
            .toList.sync()
            .foreach { edge =>
              val nextId = edge._to.asInstanceOf[Id[From]]

              // Avoid cycles in the path by checking if this node
              // is already in the current path
              if (!state.visited.contains(nextId.value)) {
                val newPath = state.path :+ edge
                val newVisited = state.visited + nextId.value

                queue.enqueue(PathState(nextId, newPath, newVisited))
                nodesAtNextLevel += 1
              }
            }
        }

        // Level transition
        if (nodesAtCurrentLevel == 0) {
          // Move to next level if we haven't found the target
          // or we've processed all nodes at the level where we found it
          if (foundTarget) {
            // Stop BFS after this level
            break()
          } else {
            currentLevel += 1
            nodesAtCurrentLevel = nodesAtNextLevel
            nodesAtNextLevel = 0
          }
        }
      }

      // Return stream of paths
      Stream.emits(result)
    }

    /**
     * Create a traversal for BFS with a single starting node
     */
    def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startId: Id[N])
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startId)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.BFS)

    /**
     * Create a traversal for BFS with a single starting node and specified depth
     */
    def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startId: Id[N],
                                                                            maxDepth: Int)
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startId)
        .withMaxDepth(maxDepth)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.BFS)

    /**
     * Create a traversal for BFS with multiple starting nodes
     */
    def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startIds: Set[Id[N]])
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startIds)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.BFS)

    /**
     * Create a traversal for BFS with multiple starting nodes and specified depth
     */
    def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startIds: Set[Id[N]], maxDepth: Int)
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startIds)
        .withMaxDepth(maxDepth)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.BFS)

    /**
     * Create a traversal for DFS
     */
    def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startId: Id[N])
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startId)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.DFS)

    /**
     * Create a traversal for DFS with specified depth
     */
    def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startId: Id[N], maxDepth: Int)
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startId)
        .withMaxDepth(maxDepth)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.DFS)

    /**
     * Create a traversal for DFS with multiple starting nodes
     */
    def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startIds: Set[Id[N]])
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startIds)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.DFS)

    /**
     * Create a traversal for DFS with multiple starting nodes and specified depth
     */
    def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](startIds: Set[Id[N]], maxDepth: Int)
                                                                           (implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
      GraphTraversal.from(startIds)
        .withMaxDepth(maxDepth)
        .follow[E, T](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .using(TraversalStrategy.DFS)

    /**
     * Compatibility method for legacy code - uses the new traversal API internally
     * but returns a backward-compatible result wrapped in a Task
     */
    def collectReachableIds[E <: EdgeDocument[E, N, N], N <: Document[N]](startId: Id[N], maxDepth: Int = Int.MaxValue)
                                                                         (implicit ev: Doc =:= E): Task[Set[Id[N]]] =
      GraphTraversal.from(startId)
        .withMaxDepth(maxDepth)
        .follow[E, N](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .targetIds
        .toList
        .map(_.toSet)
  }
}