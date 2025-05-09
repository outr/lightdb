package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeModel
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.Task

/**
 * A simple BFS‐based engine for traversals with an exact `maxDepth` limit.
 */
class BFSEngine[N <: Document[N], E <: Document[E], M <: DocumentModel[E]](
                                                                            startIds: Set[Id[N]],
                                                                            via: GraphStep[E, M, N, N],
                                                                            maxDepth: Int
                                                                          )(implicit transaction: PrefixScanningTransaction[E, M]) {

  private def loop(frontier: Set[Id[N]], visited: Set[Id[N]], depth: Int): Task[Set[Id[N]]] =
    if (frontier.isEmpty || depth > maxDepth) {
      Task.pure(visited)
    } else {
      for {
        lists <- Task.sequence(frontier.toList.map(id => via.neighbors(id, transaction)))
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
 * Companion object with factory methods
 */
object BFSEngine {
  /**
   * Create a BFSEngine using a step function
   */
  def withStepFunction[N <: Document[N]](
                                          startIds: Set[Id[N]],
                                          step: Id[N] => Task[Set[Id[N]]],
                                          maxDepth: Int = Int.MaxValue
                                        ): StepFunctionBFSEngine[N] = {
    new StepFunctionBFSEngine[N](startIds, step, maxDepth)
  }

  /**
   * BFS engine that uses a step function
   */
  class StepFunctionBFSEngine[N <: Document[N]](
                                                 startIds: Set[Id[N]],
                                                 step: Id[N] => Task[Set[Id[N]]],
                                                 maxDepth: Int
                                               ) {
    private def loop(frontier: Set[Id[N]], visited: Set[Id[N]], depth: Int): Task[Set[Id[N]]] =
      if (frontier.isEmpty || depth > maxDepth) {
        Task.pure(visited)
      } else {
        for {
          lists <- Task.sequence(frontier.toList.map(step))
          next = lists.flatten.toSet -- visited
          out <- loop(next, visited ++ next, depth + 1)
        } yield out
      }

    /**
     * Collect all nodes reachable from the starting nodes.
     */
    def collectAllReachable(): Task[Set[Id[N]]] = loop(startIds, startIds, depth = 1)
  }

  // Add this to your BFSEngine.scala file
  implicit class BFSEngineOps[N <: Document[N], E <: Document[E], M <: DocumentModel[E]](engine: BFSEngine[N, E, M]) {
    /**
     * Execute the BFS traversal and return the reachable nodes
     */
    def through(model: EdgeModel[_, N, N]): Task[Set[Id[N]]] = {
      engine.collectAllReachable()
    }
  }
}