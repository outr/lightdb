package lightdb.traversal.graph

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.{Stream, Task}

/**
 * Primary entry point for graph traversals
 */
trait GraphTraversal {
  /**
   * Start a traversal from a single document ID
   */
  def from[D <: Document[D]](id: Id[D]): DocumentTraversalBuilder[D] = DocumentTraversalBuilder(Stream.emit(id))

  /**
   * Start a traversal from a set of document IDs
   */
  def from[D <: Document[D]](ids: Set[Id[D]]): DocumentTraversalBuilder[D] =
    DocumentTraversalBuilder(Stream.emits(ids.toSeq))

  /**
   * Start a traversal from a stream of document IDs
   */
  def fromStream[D <: Document[D]](idStream: Stream[Id[D]]): DocumentTraversalBuilder[D] =
    DocumentTraversalBuilder(idStream)

  /**
   * Create a traversal using a step function, for compatibility with legacy code
   *
   * @param startIds The set of starting node IDs
   * @param step     The step function that returns neighbors for a node
   * @param maxDepth The maximum traversal depth
   * @return A StepFunctionTraversal instance
   */
  def withStepFunction[N <: Document[N]](startIds: Set[Id[N]],
                                         step: Id[N] => Task[Set[Id[N]]],
                                         maxDepth: Int = Int.MaxValue): StepFunctionTraversal[N] =
    StepFunctionTraversal[N](startIds, step, maxDepth)

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
    tx.prefixStream(id.value)
      .filter(_._from == id)
      .map(_._to)
      .toList
      .map(_.toSet)
  }

  /**
   * A traversal that uses a step function to find neighbors
   * This provides compatibility with legacy code that used BFSEngine.withStepFunction
   */
  case class StepFunctionTraversal[N <: Document[N]](startIds: Set[Id[N]],
                                                     step: Id[N] => Task[Set[Id[N]]],
                                                     maxDepth: Int) {
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

