package lightdb.traversal.graph

/**
 * Traversal strategies
 */
sealed trait TraversalStrategy

object TraversalStrategy {
  case object BFS extends TraversalStrategy
  case object DFS extends TraversalStrategy
}

