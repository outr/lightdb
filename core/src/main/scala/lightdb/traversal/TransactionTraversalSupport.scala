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
     * Start a traversal from a node ID with a step function
     */
    def from[N <: Document[N]](id: Id[N]): StepBuilder[N] = {
      new StepBuilder[N](Set(id))
    }

    /**
     * Start a traversal from multiple node IDs with a step function
     */
    def from[N <: Document[N]](ids: Set[Id[N]]): StepBuilder[N] = {
      new StepBuilder[N](ids)
    }

    /**
     * Create a forward step for an edge document type
     */
    def forward[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                             model: EdgeModel[E, From, To]
                                                                                           )(implicit ev: Doc =:= E): StepFunction[From, To] = {
      id: Id[From] =>
        // Use prefixStream to find edges with the given from ID
        prefixStream(id.value)
          .map(ev.apply) // Convert to E
          .filter(_._from == id) // Ensure it's the correct from ID
          .map(_._to) // Extract the to IDs
          .toList // Collect as a list
          .map(_.toSet) // Convert to a set
    }

    /**
     * Create a reverse step for an edge document type
     */
    def reverse[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                             model: EdgeModel[E, From, To]
                                                                                           )(implicit ev: Doc =:= E): StepFunction[To, From] = {
      id: Id[To] =>
        // For reverse traversal, we need to scan all edges and filter
        // This is less efficient but necessary for reverse lookup
        prefixStream("")
          .map(ev.apply) // Convert to E
          .filter(_._to == id) // Find edges pointing to the given ID
          .map(_._from) // Extract the from IDs
          .toList // Collect as a list
          .map(_.toSet) // Convert to a set
    }

    /**
     * Create a BFS engine from multiple starting nodes
     */
    def bfs[E <: EdgeDocument[E, N, N], N <: Document[N]](
                                                           startIds: Set[Id[N]],
                                                           maxDepth: Int = Int.MaxValue
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
     * Type alias for a step function
     */
    type StepFunction[From <: Document[From], To <: Document[To]] = Id[From] => Task[Set[Id[To]]]

    /**
     * Builder for step-based traversal
     */
    class StepBuilder[N <: Document[N]](private val startIds: Set[Id[N]]) {
      /**
       * Perform breadth-first search using a step function
       */
      def bfs[To <: Document[To]](
                                   step: StepFunction[N, To],
                                   maxDepth: Int = Int.MaxValue
                                 ): BFSResult[N, To] = {
        new BFSResult[N, To](startIds, step, maxDepth)
      }
    }

    /**
     * Result of a BFS traversal
     */
    class BFSResult[From <: Document[From], To <: Document[To]](
                                                                 private val startIds: Set[Id[From]],
                                                                 private val step: StepFunction[From, To],
                                                                 private val maxDepth: Int
                                                               ) {
      private def loop(frontier: Set[Id[From]], visited: Set[Id[To]], depth: Int): Task[Set[Id[To]]] = {
        if (frontier.isEmpty || depth > maxDepth) {
          Task.pure(visited)
        } else {
          for {
            nextSets <- Task.sequence(frontier.toList.map(step))
            next = nextSets.flatten.toSet -- visited
            out <- loop(next.asInstanceOf[Set[Id[From]]], visited ++ next, depth + 1)
          } yield out
        }
      }

      /**
       * Collect all nodes reachable from the starting nodes
       */
      def collectAllReachable(): Task[Set[Id[To]]] = {
        // For same-type traversal, include startIds in visited set
        val initialVisited = if (startIds.headOption.exists(_.getClass == classOf[Id[To]])) {
          startIds.asInstanceOf[Set[Id[To]]]
        } else {
          Set.empty[Id[To]]
        }

        loop(startIds, initialVisited, depth = 1)
      }
    }
  }
}