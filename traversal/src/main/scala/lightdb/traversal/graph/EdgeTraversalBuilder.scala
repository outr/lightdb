package lightdb.traversal.graph

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.{Pull, Step, Stream, Task}

/**
 * Builder for edge traversals
 *
 * @param fromIds  The stream of document IDs to start the traversal from
 * @param tx       A transaction that supports prefix scanning for the edge type
 * @param maxDepth The maximum traversal depth
 */
case class EdgeTraversalBuilder[E <: EdgeDocument[E, F, T], F <: Document[F], T <: Document[T]](
  fromIds: Stream[Id[F]],
  tx: PrefixScanningTransaction[E, _],
  maxDepth: Int,
  edgeFilter: E => Boolean = (_: E) => true,
  strategy: TraversalStrategy = TraversalStrategy.BFS
) {
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
    import scala.collection.mutable

    Stream(
      Task.defer {
        val lock = new AnyRef

        // BFS queue carries (node, depth)
        val q: mutable.Queue[(Id[F], Int)] = mutable.Queue.empty
        val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

        // Materialize initial frontier once (typically tiny).
        val starts: List[Id[F]] = fromIds.toList.sync()
        starts.foreach { id =>
          visited.put(id, true)
          q.enqueue(id -> 1)
        }

        var currentDepth: Int = 0
        var currentPull: Pull[E] = Pull.fromList(Nil)
        var currentPullInitialized: Boolean = false
        var done: Boolean = false

        def closeCurrentPull(): Unit = {
          try currentPull.close.handleError(_ => Task.unit).sync()
          catch { case _: Throwable => () }
        }

        def fetchNextNode(): Boolean = {
          closeCurrentPull()
          if (q.isEmpty) {
            done = true
            currentPull = Pull.fromList(Nil)
            currentPullInitialized = true
            false
          } else {
            val (id, depth) = q.dequeue()
            currentDepth = depth
            val edgesStream: Stream[E] = tx
              .prefixStream(id.value)
              .asInstanceOf[Stream[E]]
              .filter(_._from == id)
              .filter(edgeFilter)
            currentPull = Stream.task(edgesStream).sync()
            currentPullInitialized = true
            true
          }
        }

        val pullTask: Task[Step[E]] = Task {
          lock.synchronized {
            @annotation.tailrec
            def loop(): Step[E] = {
              if (done) {
                Step.Stop
              } else {
                if (!currentPullInitialized) {
                  fetchNextNode()
                }

                currentPull.pull.sync() match {
                  case e @ Step.Emit(edge) =>
                    // Expand frontier if we have remaining depth.
                    if (currentDepth < maxDepth) {
                      val targetId = edge._to.asInstanceOf[Id[F]]
                      if (Option(visited.putIfAbsent(targetId, true)).isEmpty) {
                        q.enqueue(targetId -> (currentDepth + 1))
                      }
                    }
                    e
                  case Step.Skip =>
                    loop()
                  case Step.Concat(inner) =>
                    currentPull = inner
                    loop()
                  case Step.Stop =>
                    // Current node drained; move to next.
                    currentPullInitialized = false
                    loop()
                }
              }
            }
            loop()
          }
        }

        val closeTask: Task[Unit] = Task {
          lock.synchronized {
            done = true
            closeCurrentPull()
          }
        }

        Task.pure(Pull(pullTask, closeTask))
      }
    )
  }

  /**
   * Execute a DFS traversal that returns a stream of edges
   */
  private def executeDFSEdges(): Stream[E] = {
    // Iterative DFS (stack) for the same reasons as BFS: avoid recursive Stream.append chains.
    import scala.collection.mutable

    Stream(
      Task.defer {
        val lock = new AnyRef

        // DFS stack carries (node, depth)
        val stack: mutable.ArrayDeque[(Id[F], Int)] = mutable.ArrayDeque.empty
        val visited = new java.util.concurrent.ConcurrentHashMap[Id[F], Boolean]()

        val starts: List[Id[F]] = fromIds.toList.sync()
        // Push in reverse so iteration order is stable-ish.
        starts.reverse.foreach { id =>
          visited.put(id, true)
          stack.prepend(id -> 1)
        }

        var currentDepth: Int = 0
        var currentPull: Pull[E] = Pull.fromList(Nil)
        var currentPullInitialized: Boolean = false
        var done: Boolean = false

        def closeCurrentPull(): Unit = {
          try currentPull.close.handleError(_ => Task.unit).sync()
          catch { case _: Throwable => () }
        }

        def fetchNextNode(): Boolean = {
          closeCurrentPull()
          if (stack.isEmpty) {
            done = true
            currentPull = Pull.fromList(Nil)
            currentPullInitialized = true
            false
          } else {
            val (id, depth) = stack.removeHead()
            currentDepth = depth
            val edgesStream: Stream[E] = tx
              .prefixStream(id.value)
              .asInstanceOf[Stream[E]]
              .filter(_._from == id)
              .filter(edgeFilter)
            currentPull = Stream.task(edgesStream).sync()
            currentPullInitialized = true
            true
          }
        }

        val pullTask: Task[Step[E]] = Task {
          lock.synchronized {
            @annotation.tailrec
            def loop(): Step[E] = {
              if (done) {
                Step.Stop
              } else {
                if (!currentPullInitialized) {
                  fetchNextNode()
                }

                currentPull.pull.sync() match {
                  case e @ Step.Emit(edge) =>
                    if (currentDepth < maxDepth) {
                      val targetId = edge._to.asInstanceOf[Id[F]]
                      if (Option(visited.putIfAbsent(targetId, true)).isEmpty) {
                        stack.prepend(targetId -> (currentDepth + 1))
                      }
                    }
                    e
                  case Step.Skip =>
                    loop()
                  case Step.Concat(inner) =>
                    currentPull = inner
                    loop()
                  case Step.Stop =>
                    currentPullInitialized = false
                    loop()
                }
              }
            }
            loop()
          }
        }

        val closeTask: Task[Unit] = Task {
          lock.synchronized {
            done = true
            closeCurrentPull()
          }
        }

        Task.pure(Pull(pullTask, closeTask))
      }
    )
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

