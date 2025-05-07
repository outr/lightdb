package lightdb.transaction

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import rapid.{Pull, Task}

trait PrefixScanningTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  def jsonPrefixStream(prefix: String): rapid.Stream[Json]

  def prefixStream(prefix: String): rapid.Stream[Doc] = jsonPrefixStream(prefix).map(_.as[Doc](store.model.rw))

  def prefixList(prefix: String): Task[List[Doc]] = prefixStream(prefix).toList

  ///// Edge and Traversal Functionality /////

  def edgesFor[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](from: Id[From])
                                                                                          (implicit ev: Doc =:= E): rapid.Stream[E] =
    prefixStream(from.value).map(ev.apply)

  def reachableFrom[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From],
                                                                              maxDepth: Int = Int.MaxValue)
                                                                             (implicit ev: Doc =:= E): rapid.Stream[E] = {
    var visited = Set.empty[Id[From]]

    def recurse(queue: Set[Id[From]], depth: Int): rapid.Stream[E] = {
      if (queue.isEmpty || depth >= maxDepth) {
        rapid.Stream.empty
      } else {
        val head = queue.head
        val rest = queue.tail

        edgesFor[E, From, From](head).flatMap { edge =>
          val to = edge._to
          if (visited.contains(to)) {
            rapid.Stream.empty
          } else {
            visited += to
            rapid.Stream.emit(edge).append(recurse(Set(to), depth + 1))
          }
        }.append(recurse(rest, depth))
      }
    }

    recurse(Set(from), 0)
  }

  def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From],
                                                                         to: Id[From],
                                                                         maxDepth: Int,
                                                                         bufferSize: Int = 100,
                                                                         edgeFilter: E => Boolean = (_: E) => true)
                                                                        (implicit ev: Doc =:= E): rapid.Stream[TraversalPath[E, From]] = {
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
              val edges: List[E] = edgesFor[E, From, From](currentId).toList.sync()
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

    rapid.Stream(Task.pure(pull))
  }

  def shortestPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](from: Id[From],
                                                                              to: Id[From],
                                                                              maxDepth: Int = Int.MaxValue,
                                                                              bufferSize: Int = 100,
                                                                              edgeFilter: E => Boolean = (_: E) => true)
                                                                             (implicit ev: Doc =:= E): rapid.Stream[TraversalPath[E, From]] =
    allPaths[E, From](from, to, maxDepth, bufferSize, edgeFilter)
      .takeWhileWithFirst((first, current) => current.edges.length == first.edges.length)
}

case class TraversalPath[E <: EdgeDocument[E, From, From], From <: Document[From]](edges: List[E]) {
  def nodes: List[Id[From]] = edges match {
    case Nil => Nil
    case _ => edges.head._from :: edges.map(_._to)
  }
}
