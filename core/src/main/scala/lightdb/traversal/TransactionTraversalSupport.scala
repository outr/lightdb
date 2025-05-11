package lightdb.traversal

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
  object traversal {
    /**
     * Get a stream of edges for the specified from ID
     */
    def edgesFor[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](fromId: Id[From])
                                                                                            (implicit ev: Doc =:= E): Stream[E] = {
      // Use ev to convert the transaction to the correct type
      prefixStream(fromId.value).map(ev.apply)
    }

    /**
     * Find all nodes reachable from a starting ID by following edges
     */
    def reachableFrom[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From], maxDepth: Int = Int.MaxValue)
                                                                               (implicit ev: Doc =:= E): Stream[E] = {
      RecursiveTraversal.reachableFrom[E, From](from, maxDepth)(edgesFor[E, From, From])
    }

    /**
     * Find all paths between two nodes
     */
    def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From],
                                                                           to: Id[From],
                                                                           maxDepth: Int,
                                                                           bufferSize: Int = 100,
                                                                           edgeFilter: E => Boolean = (_: E) => true)
                                                                          (implicit ev: Doc =:= E): Stream[TraversalPath[E, From]] = {
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
    def shortestPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From],
                                                                                to: Id[From],
                                                                                maxDepth: Int = Int.MaxValue,
                                                                                bufferSize: Int = 100,
                                                                                edgeFilter: E => Boolean = (_: E) => true)
                                                                               (implicit ev: Doc =:= E): Stream[TraversalPath[E, From]] = {
      allPaths[E, From](from, to, maxDepth, bufferSize, edgeFilter)
        .takeWhileWithFirst((first, current) => current.edges.length == first.edges.length)
    }

    /**
     * Create a BFS engine for graph traversal with a single starting node
     */
    def bfs[E <: EdgeDocument[E, N, N], N <: Document[N]](
                                                           startId: Id[N]
                                                         )(implicit ev: Doc =:= E): BFSEngine[N, Doc, Model] = {
      bfs[E, N](startId, Int.MaxValue)
    }

    /**
     * Create a BFS engine for graph traversal with a single starting node and specified depth
     */
    def bfs[E <: EdgeDocument[E, N, N], N <: Document[N]](
                                                           startId: Id[N],
                                                           maxDepth: Int
                                                         )(implicit ev: Doc =:= E): BFSEngine[N, Doc, Model] = {
      // Create a GraphStep that uses edges for traversal
      val step = new GraphStep[Doc, Model, N, N] {
        def neighbors(id: Id[N], tx: PrefixScanningTransaction[Doc, Model]): Task[Set[Id[N]]] = {
          prefixStream(id.value)
            .map(ev.apply) // Convert to E
            .filter(_._from == id) // Ensure it's the correct from ID
            .map(_._to) // Extract the to IDs
            .toList // Collect as a list
            .map(_.toSet) // Convert to a set
        }
      }
      new BFSEngine(Set(startId), step, maxDepth)(self)
    }

    /**
     * Create a BFS engine with multiple starting nodes
     */
    def bfs[E <: EdgeDocument[E, N, N], N <: Document[N]](
                                                           startIds: Set[Id[N]]
                                                         )(implicit ev: Doc =:= E): BFSEngine[N, Doc, Model] = {
      bfs[E, N](startIds, Int.MaxValue)
    }

    /**
     * Create a BFS engine with multiple starting nodes and specified depth
     */
    def bfs[E <: EdgeDocument[E, N, N], N <: Document[N]](
                                                           startIds: Set[Id[N]],
                                                           maxDepth: Int
                                                         )(implicit ev: Doc =:= E): BFSEngine[N, Doc, Model] = {
      // Create a GraphStep that uses edges for traversal
      val step = new GraphStep[Doc, Model, N, N] {
        def neighbors(id: Id[N], tx: PrefixScanningTransaction[Doc, Model]): Task[Set[Id[N]]] = {
          prefixStream(id.value)
            .map(ev.apply) // Convert to E
            .filter(_._from == id) // Ensure it's the correct from ID
            .map(_._to) // Extract the to IDs
            .toList // Collect as a list
            .map(_.toSet) // Convert to a set
        }
      }
      new BFSEngine(startIds, step, maxDepth)(self)
    }

    /**
     * Create a BFS engine with a custom GraphStep
     */
    def bfs[N <: Document[N]](
                               startId: Id[N],
                               via: GraphStep[Doc, Model, N, N]
                             ): BFSEngine[N, Doc, Model] = {
      bfs(Set(startId), via, Int.MaxValue)
    }

    /**
     * Create a BFS engine with a custom GraphStep and specified depth
     */
    def bfs[N <: Document[N]](
                               startId: Id[N],
                               via: GraphStep[Doc, Model, N, N],
                               maxDepth: Int
                             ): BFSEngine[N, Doc, Model] = {
      bfs(Set(startId), via, maxDepth)
    }

    /**
     * Create a BFS engine with multiple starting nodes and a custom GraphStep
     */
    def bfs[N <: Document[N]](
                               startIds: Set[Id[N]],
                               via: GraphStep[Doc, Model, N, N]
                             ): BFSEngine[N, Doc, Model] = {
      bfs(startIds, via, Int.MaxValue)
    }

    /**
     * Create a BFS engine with multiple starting nodes, a custom GraphStep, and specified depth
     */
    def bfs[N <: Document[N]](
                               startIds: Set[Id[N]],
                               via: GraphStep[Doc, Model, N, N],
                               maxDepth: Int
                             ): BFSEngine[N, Doc, Model] = {
      new BFSEngine(startIds, via, maxDepth)(self)
    }
  }
}