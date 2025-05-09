package lightdb.traversal

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import rapid.{Pull, Stream, Task}

/**
 * Object containing implementations for recursive traversal algorithms
 */
object RecursiveTraversal {
  /**
   * Find all nodes reachable from a starting ID by following edges
   *
   * @param from         The starting ID
   * @param maxDepth     The maximum traversal depth
   * @param edgesForFunc A function that returns edges for a given ID
   * @return A stream of all edges reachable from the starting ID
   */
  def reachableFrom[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                               from: Id[From],
                                                                               maxDepth: Int = Int.MaxValue
                                                                             )(edgesForFunc: Id[From] => Stream[E]): Stream[E] = {
    // Use an AtomicReference to handle the visited set in a functional way
    import java.util.concurrent.atomic.AtomicReference
    val visitedRef = new AtomicReference(Set.empty[Id[From]])

    def recurse(queue: Set[Id[From]], depth: Int): Stream[E] = {
      if (queue.isEmpty || depth >= maxDepth) {
        Stream.empty
      } else {
        val head = queue.head
        val rest = queue.tail

        edgesForFunc(head).flatMap { edge =>
          val to = edge._to

          // Thread-safe check and update of visited set
          var emitAndRecurse = false
          visitedRef.getAndUpdate { visited =>
            if (visited.contains(to)) {
              visited // No change
            } else {
              emitAndRecurse = true
              visited + to
            }
          }

          if (emitAndRecurse) {
            Stream.emit(edge).append(recurse(Set(to), depth + 1))
          } else {
            Stream.empty
          }
        }.append(recurse(rest, depth))
      }
    }

    // Start the recursion without marking the starting node as visited
    // to match the original behavior
    recurse(Set(from), 0)
  }

  /**
   * Find all paths between two nodes
   *
   * @param from         The starting node ID
   * @param to           The target node ID
   * @param maxDepth     Maximum path depth
   * @param bufferSize   Size of the buffer for collecting paths
   * @param edgeFilter   Filter for edges to consider
   * @param edgesForFunc Function to retrieve edges for a node
   * @return A stream of all paths from the starting node to the target node
   */
  def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](
                                                                          from: Id[From],
                                                                          to: Id[From],
                                                                          maxDepth: Int,
                                                                          bufferSize: Int = 100,
                                                                          edgeFilter: E => Boolean = (_: E) => true
                                                                        )(edgesForFunc: Id[From] => Stream[E]): Stream[TraversalPath[E, From]] = {
    import scala.collection.mutable

    val queue = mutable.Queue[(Id[From], List[E])]()
    val seen = mutable.Set[List[Id[From]]]()
    queue.enqueue((from, Nil))

    val pull: Pull[TraversalPath[E, From]] = new Pull[TraversalPath[E, From]] {
      private var buffer: List[TraversalPath[E, From]] = Nil

      override def pull(): Option[TraversalPath[E, From]] = {
        if (buffer.nonEmpty) {
          val next = buffer.head
          buffer = buffer.tail
          Some(next)
        } else {
          var collected = List.empty[TraversalPath[E, From]]

          while (queue.nonEmpty && collected.size < bufferSize) {
            val (currentId, path) = queue.dequeue()

            if (path.length < maxDepth) {
              val edges: List[E] = edgesForFunc(currentId).toList.sync()
              val filteredEdges = edges.filter(edgeFilter)
              val nextSteps = filteredEdges.filterNot(e => path.exists(_._to == e._to))
              val newPaths = nextSteps.map(e => (e._to, path :+ e))
              val (completed, pending) = newPaths.partition(_._1 == to)

              pending.foreach {
                case (id, newPath) =>
                  val signature = from +: newPath.map(_._to)
                  if (!seen.contains(signature)) {
                    seen += signature
                    queue.enqueue((id, newPath))
                  }
              }

              collected ++= completed.map(p => TraversalPath(p._2))
            }
          }

          if (collected.nonEmpty) {
            buffer = collected.tail
            Some(collected.head)
          } else {
            None
          }
        }
      }
    }

    Stream(Task.pure(pull))
  }
}
