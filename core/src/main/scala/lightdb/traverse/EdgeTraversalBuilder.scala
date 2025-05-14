package lightdb.traverse

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.{Stream, Task}

/**
 * Builder for edge traversals
 *
 * @param fromIds  The stream of document IDs to start the traversal from
 * @param tx       A transaction that supports prefix scanning for the edge type
 * @param maxDepth The maximum traversal depth
 */
case class EdgeTraversalBuilder[E <: EdgeDocument[E, F, T], F <: Document[F], T <: Document[T]](fromIds: Stream[Id[F]],
                                                                                                tx: PrefixScanningTransaction[E, _],
                                                                                                maxDepth: Int,
                                                                                                edgeFilter: E => Boolean = (_: E) => true,
                                                                                                strategy: TraversalStrategy = TraversalStrategy.BFS) {
  /**
   * Configure the maximum traversal depth
   */
  def withMaxDepth(depth: Int): EdgeTraversalBuilder[E, F, T] = copy(maxDepth = depth)

  /**
   * Configure edge filtering
   *
   * @param predicate A predicate function to filter edges
   */
  def filter(predicate: E => Boolean): EdgeTraversalBuilder[E, F, T] = copy(edgeFilter = predicate)

  /**
   * Configure traversal strategy
   *
   * @param traversalStrategy The traversal strategy to use
   */
  def using(traversalStrategy: TraversalStrategy): EdgeTraversalBuilder[E, F, T] = copy(strategy = traversalStrategy)

  /**
   * Follow additional edges from the targets of the current traversal
   *
   * @tparam E2 The next edge document type
   * @tparam T2 The next target document type
   * @param nextTx A transaction that supports prefix scanning for the next edge type
   * @return A builder for the next edge traversal
   */
  def follow[E2 <: EdgeDocument[E2, T, T2], T2 <: Document[T2]](nextTx: PrefixScanningTransaction[E2, _]): EdgeTraversalBuilder[E2, T, T2] =
    EdgeTraversalBuilder[E2, T, T2](edges.map(_._to).distinct, nextTx, maxDepth)

  /**
   * Get the stream of edge documents
   *
   * @return A stream of edge documents
   */
  def edges: Stream[E] = strategy match {
    case TraversalStrategy.BFS => executeBFSEdges()
    case TraversalStrategy.DFS => executeDFSEdges()
  }

  /**
   * Get the stream of target document IDs
   *
   * @return A stream of target document IDs
   */
  def targetIds(implicit ev: F =:= T): Stream[Id[T]] = fromIds.map(_.coerce[T]).append(edges.map(_._to)).distinct

  /**
   * Get the stream of target documents
   *
   * @param docTx A transaction for retrieving target documents
   * @return A stream of target documents
   */
  def documents(docTx: Transaction[T, _]): Stream[T] = edges.map(_._to).distinct.evalMap(id => docTx(id))

  /**
   * Find all paths to a target node
   *
   * @param target The ID of the target node
   * @return A stream of paths to the target node
   */
  def findPaths(target: Id[T]): Stream[TraversalPath[E, F, T]] = implementPathFinding(target, findAll = true)

  /**
   * Find the shortest path to a target node
   *
   * @param target The ID of the target node
   * @return A stream containing the shortest path to the target node, if any
   */
  def findShortestPath(target: Id[T]): Stream[TraversalPath[E, F, T]] = implementPathFinding(target, findAll = false)

  /**
   * Execute a BFS traversal that returns a stream of edges
   */
  private def executeBFSEdges(): Stream[E] = {
    // We'll use a concurrent set to track visited nodes
    val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

    // Function to process a level of the BFS
    def processLevel(frontier: Stream[Id[F]], depth: Int): Stream[E] = {
      if (depth > maxDepth) {
        Stream.empty
      } else {
        // Process the current frontier
        val edgesStream = frontier.flatMap { id =>
          tx.prefixStream(id.value)
            .filter(_._from == id)
            .filter(edgeFilter)
        }

        // Collect target IDs for the next frontier
        edgesStream.flatMap { edge =>
          val targetId = edge._to.asInstanceOf[Id[F]] // Safe cast for reflexive graphs

          // Emit the edge, and if targetId hasn't been visited, include it in next level
          Stream.emit(edge).append {
            // Use Option to safely handle null - None means the key wasn't in the map
            Option(visited.putIfAbsent(targetId, true)) match {
              case None =>
                // The key wasn't previously in the map (null was returned)
                // We'll create a stream with just this one ID for the next level
                val nextFrontierStream = Stream.emit(targetId)

                // Process the next level recursively - no need for Stream.defer as Stream is already lazy
                processLevel(nextFrontierStream, depth + 1)
              case Some(_) =>
                // The key was already in the map
                Stream.empty // Already visited, so don't include in next level
            }
          }
        }
      }
    }

    // Mark starting nodes as visited and begin traversal with a stream of ids
    fromIds.foreach { id =>
      visited.put(id, true)
    }.flatMap { id =>
      processLevel(Stream.emit(id), 1)
    }
  }

  /**
   * Execute a DFS traversal that returns a stream of edges
   */
  private def executeDFSEdges(): Stream[E] = {
    // We'll use a concurrent set to track visited nodes
    val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

    // Recursive function to perform DFS
    def dfs(id: Id[F], depth: Int): Stream[E] = {
      if (depth > maxDepth) {
        Stream.empty
      } else {
        // Get edges from the current node
        val edgesFromNode = tx.prefixStream(id.value)
          .asInstanceOf[Stream[E]]
          .filter(_._from == id)
          .filter(edgeFilter)

        // For each edge, emit it and then visit its target
        edgesFromNode.flatMap { edge =>
          val targetId = edge._to.asInstanceOf[Id[F]] // Safe cast for reflexive graphs

          // Emit the edge, then recursively visit the target if not visited
          Stream.emit(edge).append {
            // Use Option to safely handle null - None means the key wasn't in the map
            Option(visited.putIfAbsent(targetId, true)) match {
              case None =>
                // The key wasn't previously in the map (null was returned)
                // Stream is already lazy, so no need for Stream.defer
                dfs(targetId, depth + 1)
              case Some(_) =>
                // The key was already in the map
                Stream.empty
            }
          }
        }
      }
    }

    // Mark starting nodes as visited and begin traversal
    fromIds.foreach { id =>
      visited.put(id, true)
    }.flatMap { id =>
      dfs(id, 1)
    }
  }

  /**
   * Implement path finding (both all paths and shortest path)
   *
   * @param target  The target node ID
   * @param findAll Whether to find all paths or just the shortest
   * @return A stream of paths
   */
  private def implementPathFinding(target: Id[T], findAll: Boolean): Stream[TraversalPath[E, F, T]] = {
    import scala.collection.mutable

    // Path data includes current node and edges taken to get there
    case class PathData(currentId: Id[F], edges: List[E])

    // We'll use a breadth-first approach but emit paths as they're found
    def findPaths(): Stream[TraversalPath[E, F, T]] = {
      // Set up mutable state for the search
      val queue = mutable.Queue[PathData]()
      val visited = mutable.Set[Id[F]]()
      var shortestLength: Option[Int] = None

      // Initialize queue with starting nodes
      fromIds.toList.sync().foreach { id =>
        queue.enqueue(PathData(id, Nil))
        visited.add(id)
      }

      // Create a stream that produces paths as they're found
      def processQueue(): Stream[TraversalPath[E, F, T]] = {
        if (queue.isEmpty) {
          Stream.empty
        } else {
          // Dequeue the next path to explore
          val PathData(currentId, pathEdges) = queue.dequeue()

          // If we're only finding shortest paths and already have a shorter one, skip
          if (shortestLength.exists(pathEdges.length >= _)) {
            // Continue with next item in queue
            processQueue()
          } else {
            // Get edges from current node
            val outgoingEdges = tx.prefixStream(currentId.value)
              .asInstanceOf[Stream[E]]
              .filter(_._from == currentId)
              .filter(edgeFilter)
              .toList.sync() // Collect to avoid re-execution

            // Find paths that reach the target
            val completedPaths = outgoingEdges
              .filter(_._to == target)
              .map(edge => new TraversalPath[E, F, T](pathEdges :+ edge))

            // If we found paths to target and only want shortest, update shortestLength
            if (!findAll && completedPaths.nonEmpty) {
              val newLength = completedPaths.head.edges.length
              shortestLength = Some(shortestLength.fold(newLength)(len => math.min(len, newLength)))
            }

            // If not at max depth, enqueue next level paths
            if (pathEdges.length < maxDepth) {
              // Add paths to unexplored nodes
              outgoingEdges.foreach { edge =>
                val targetId = edge._to.asInstanceOf[Id[F]] // Safe cast for reflexive graphs

                // Only follow path if we haven't visited this node in this path
                // For BFS path finding, we track visited nodes per path
                val pathVisited = pathEdges.exists(_._to == targetId)
                if (!pathVisited && targetId != target) { // Don't explore beyond target
                  queue.enqueue(PathData(targetId, pathEdges :+ edge))
                }
              }
            }

            // Emit completed paths and continue processing queue
            if (completedPaths.isEmpty) {
              processQueue()
            } else {
              Stream.emits(completedPaths).append(
                processQueue()
              )
            }
          }
        }
      }

      processQueue()
    }

    findPaths()
  }
}