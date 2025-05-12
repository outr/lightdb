package lightdb.traversal

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.{Stream, Task}

import scala.collection.mutable

/**
 * A standalone shortestPath implementation that focuses on clarity and correctness
 * over performance optimizations
 */
object SimpleShortestPathFinder {
  /**
   * Find shortest paths between two nodes using BFS
   * Materializes the whole result before returning a stream
   */
  def findShortestPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                                     from: Id[From],
                                                                                                     to: Id[To],
                                                                                                     transaction: PrefixScanningTransaction[E, _],
                                                                                                     maxDepth: Int = Int.MaxValue,
                                                                                                     edgeFilter: E => Boolean = (_: E) => true
                                                                                                   ): Stream[TraversalPath[E, From, To]] = {
    // Use single-class BFS for simplicity
    val results = materializeShortestPaths(from, to, transaction, maxDepth, edgeFilter)
    Stream.emits(results)
  }

  // Basic path information
  private case class PathInfo[E, F, T](
                                        current: Id[F],
                                        edges: List[E],
                                        visited: Set[String] // Using String for node IDs to avoid type issues
                                      )

  /**
   * A simple, imperative BFS implementation for finding shortest paths
   * Returns a fully materialized list to avoid streaming issues
   */
  private def materializeShortestPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                                                    from: Id[From],
                                                                                                                    to: Id[To],
                                                                                                                    tx: PrefixScanningTransaction[E, _],
                                                                                                                    maxDepth: Int,
                                                                                                                    edgeFilter: E => Boolean
                                                                                                                  ): List[TraversalPath[E, From, To]] = {
    val paths = mutable.ListBuffer.empty[TraversalPath[E, From, To]]
    val queue = mutable.Queue.empty[PathInfo[E, From, To]]

    // Helper for path key (nodePath)
    def nodeKey(id: Id[_]): String = id.value

    // Initialize with start node
    queue.enqueue(PathInfo(from, Nil, Set(nodeKey(from))))

    var depth = 0
    var levelSize = queue.size
    var nextLevelSize = 0
    var foundAtThisLevel = false

    // Simple, straightforward BFS
    while (queue.nonEmpty && depth <= maxDepth && !foundAtThisLevel) {
      val info = queue.dequeue()
      levelSize -= 1

      // Check if reached target
      if (info.current.asInstanceOf[Id[To]] == to) {
        paths += new TraversalPath[E, From, To](info.edges.asInstanceOf[List[E]])
        foundAtThisLevel = true
      }
      else if (depth < maxDepth) {
        // Get all outgoing edges
        val edges = tx.prefixStream(info.current.value)
          .asInstanceOf[Stream[E]]
          .filter(_._from == info.current)
          .filter(edgeFilter)
          .toList.sync()

        // Enqueue next nodes
        for (edge <- edges) {
          val nextId = edge._to.asInstanceOf[Id[From]]
          val nextKey = nodeKey(nextId)

          // Avoid cycles in path
          if (!info.visited.contains(nextKey)) {
            val newEdges = info.edges :+ edge
            val newVisited = info.visited + nextKey

            queue.enqueue(PathInfo(nextId, newEdges, newVisited))
            nextLevelSize += 1
          }
        }
      }

      // Level transition
      if (levelSize == 0) {
        // If we found target at this level, finish processing the level
        if (foundAtThisLevel) {
          while (queue.nonEmpty) {
            val info = queue.dequeue()

            // Only check other nodes at this same level
            if (info.edges.length == depth) {
              if (info.current.asInstanceOf[Id[To]] == to) {
                paths += new TraversalPath[E, From, To](info.edges.asInstanceOf[List[E]])
              }
            }
          }
        } else {
          // Move to the next level
          depth += 1
          levelSize = nextLevelSize
          nextLevelSize = 0
        }
      }
    }

    paths.toList
  }
}