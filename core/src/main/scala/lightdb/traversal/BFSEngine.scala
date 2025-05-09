package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

/**
 * A simple BFS‐based engine for one‐step traversals (From == To)
 * with an exact `maxDepth` limit.
 */
class BFSEngine[N <: Document[N], E <: Document[E], M <: DocumentModel[E]](
                                                                            startIds: Set[Id[N]],
                                                                            via: GraphStep[E, M, N, N],
                                                                            maxDepth: Int
                                                                          )(implicit tx: Transaction[E, M]) {

  private def loop(frontier: Set[Id[N]], visited: Set[Id[N]], depth: Int): Task[Set[Id[N]]] =
    if (frontier.isEmpty || depth > maxDepth) {
      Task.pure(visited)
    } else {
      for {
        lists <- Task.sequence(frontier.toList.map(via.neighbors))
        next = lists.flatten.toSet -- visited
        out <- loop(next, visited ++ next, depth + 1)
      } yield out
    }

  /**
   * Collect all nodes reachable from the starting nodes.
   */
  def collectAllReachable(): Task[Set[Id[N]]] = loop(startIds, startIds, depth = 1)
}

/**
 * Integration with TraversalBuilder to create a BFSEngine from traversal parameters.
 */
object BFSEngineFactory {
  /**
   * Create a BFSEngine from a TraversalBuilder configuration.
   */
  def fromTraversalConfig[
    N <: Document[N],
    E <: Document[E],
    M <: DocumentModel[E]
  ](
     startIds: Set[Id[N]],
     maxDepth: Int,
     via: GraphStep[E, M, N, N]
   )(implicit tx: Transaction[E, M]): BFSEngine[N, E, M] = {
    new BFSEngine(startIds, via, maxDepth)
  }
}

/**
 * Extensions for TraversalBuilder to integrate with BFSEngine.
 */
trait BFSTraversalSupport {
  /**
   * Extension methods for TraversalBuilder
   */
  implicit class BFSTraversalBuilderOps[T <: Document[T]](traversalBuilder: TraversalBuilder[T]) {
    /**
     * Create a BFS traversal engine from this traversal configuration.
     */
    def toBFS[E <: Document[E], M <: DocumentModel[E]](
                                                        startIds: Set[Id[T]],
                                                        via: GraphStep[E, M, T, T],
                                                        maxDepth: Int = 10
                                                      )(implicit tx: Transaction[E, M]): BFSEngine[T, E, M] = {
      BFSEngineFactory.fromTraversalConfig(startIds, maxDepth, via)
    }
  }
}