package lightdb.traverse

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.{Stream, Task}

/**
 * Extension methods for transactions to support traversal functionality
 */
trait TransactionTraversalSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  self: PrefixScanningTransaction[Doc, Model] =>

  /**
   * Provides traversal functionality
   */
  object traverse {
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
     * Find shortest paths between two nodes
     */
    def shortestPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                                  to: Id[To],
                                                                                                  maxDepth: Int = Int.MaxValue,
                                                                                                  bufferSize: Int = 100,
                                                                                                  edgeFilter: E => Boolean = (_: E) => true)
                                                                                                 (implicit ev: Doc =:= E): Stream[TraversalPath[E, From, To]] =
      GraphTraversal.from(from)
        .withMaxDepth(maxDepth)
        .follow[E, To](self.asInstanceOf[PrefixScanningTransaction[E, _]])
        .filter(edgeFilter)
        .findShortestPath(to)

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