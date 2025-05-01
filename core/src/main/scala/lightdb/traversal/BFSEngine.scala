package lightdb.traversal

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction
import rapid.Task

/**
 * A simple BFS‐based engine for one‐step traversals (From == To)
 * with an exact `maxDepth` limit.
 */
class BFSEngine[N <: Document[N], E <: Document[E], M <: DocumentModel[E]](startIds: Set[Id[N]],
                                                    via: GraphStep[E, M, N, N],
                                                    maxDepth: Int)(implicit tx: Transaction[E, M]) {
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

  def collectAllReachable(): Task[Set[Id[N]]] = loop(startIds, startIds, depth = 1)
}