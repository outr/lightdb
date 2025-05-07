package lightdb.graph

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.{Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import rapid.{Task, logger}

import scala.language.implicitConversions

trait EdgeModel[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends DocumentModel[Doc] {
  val _from: UniqueIndex[Doc, Id[From]] = field.unique("_from", _._from)
  val _to: UniqueIndex[Doc, Id[To]] = field.unique("_to", _._to)

  def prefix(_from: Id[From], _to: Id[To] = null, extra: String = null): String = if (_to == null) {
    _from.value
  } else if (extra == null) {
    s"${_from.value}-${_to.value}"
  } else {
    s"${_from.value}-${_to.value}-$extra"
  }

  /*def edgesFor(id: Id[From]): Task[Set[Id[To]]] = edgesStore.transaction { implicit transaction =>
    transaction.get(id.asInstanceOf[Id[EdgeConnections[Doc, From, To]]]).map(_.map(_.to).getOrElse(Set.empty))
  }

  def reachableFrom(id: Id[From])(implicit ev: Id[To] =:= Id[From]): Task[Set[(Id[To], Int)]] = Task {
    var reachable = Set.empty[(Id[To], Int)]
    var visited = Set.empty[Id[To]]
    var queue = List(id -> 0)

    while (queue.nonEmpty) {
      val (from, distance) = queue.head
      queue = queue.tail

      val edges = edgesFor(from).sync()
      val newIds = edges.diff(visited)

      visited ++= newIds
      reachable ++= newIds.map(to => to -> (distance + 1))
      queue = queue ::: newIds.toList.map(to => ev(to) -> (distance + 1))
    }
    reachable
  }

  def shortestPath(from: Id[From], to: Id[To])(implicit ev: Id[From] =:= Id[To]): Task[List[Id[To]]] = {
    if (ev(from) == to) {
      Task.pure(List(ev(from)))
    } else Task {
      var queue: List[Id[To]] = List(ev(from))
      var visited: Set[Id[To]] = Set(ev(from))
      var parentMap: Map[Id[To], Id[To]] = Map.empty

      var found = false

      while (queue.nonEmpty && !found) {
        val current: Id[To] = queue.head
        queue = queue.tail

        val neighbors: Set[Id[To]] = edgesFor(ev.flip(current)).sync()
        val newNeighbors: Set[Id[To]] = neighbors.diff(visited)

        for (neighbor <- newNeighbors) {
          parentMap += (neighbor -> current)
          visited += neighbor
          queue = queue :+ neighbor

          if (neighbor == to) {
            found = true
            queue = Nil
          }
        }
      }

      if (!parentMap.contains(to)) {
        Nil
      } else {
        var path = List.empty[Id[To]]
        var step: Id[To] = to

        while (step != ev(from)) {
          path = step :: path
          step = parentMap(step)
        }

        ev(from) :: path
      }
    }
  }

  def shortestPaths(from: Id[From], to: Id[To])(implicit ev: Id[From] =:= Id[To]): Task[List[List[Id[To]]]] = {
    if (ev(from) == to) {
      Task.pure(List(List(ev(from))))
    } else Task {
      var queue: List[Id[To]] = List(ev(from))
      var visited: Set[Id[To]] = Set(ev(from))
      var parentMap: Map[Id[To], Set[Id[To]]] = Map.empty.withDefaultValue(Set.empty)
      var found = false

      while (queue.nonEmpty && !found) {
        val nextQueue = scala.collection.mutable.ListBuffer.empty[Id[To]]
        val newVisited = scala.collection.mutable.Set.empty[Id[To]]

        for (current <- queue) {
          val neighbors: Set[Id[To]] = edgesFor(ev.flip(current)).sync()

          for (neighbor <- neighbors if !visited.contains(neighbor)) {
            parentMap += neighbor -> (parentMap(neighbor) + current)
            newVisited += neighbor
            nextQueue += neighbor
          }
        }

        if (newVisited.contains(to)) found = true
        visited ++= newVisited
        queue = nextQueue.toList
      }

      if (!parentMap.contains(to)) {
        Nil
      } else {
        def backtrack(current: Id[To], path: List[Id[To]]): List[List[Id[To]]] = {
          if (current == ev(from)) {
            List(ev(from) :: path)
          } else {
            parentMap(current).toList.flatMap(parent => backtrack(parent, current :: path))
          }
        }
        backtrack(to, Nil)
      }
    }
  }

  def allPaths(from: Id[From], to: Id[To], maxPaths: Int, maxDepth: Int)(implicit ev: Id[From] =:= Id[To]): Task[List[List[Id[To]]]] = Task {
    val result = scala.collection.mutable.ListBuffer[List[Id[To]]]()
    val queue = scala.collection.mutable.Queue[List[Id[To]]]() // Store paths to explore
    queue.enqueue(List(ev(from))) // Start with the initial node

    while (queue.nonEmpty && result.size < maxPaths) {
      val path = queue.dequeue()
      val lastNode = path.last

      if (lastNode == to) {
        result += path // Store valid path
      } else if (path.length < maxDepth) { // Limit traversal depth
        val neighbors = edgesFor(ev.flip(lastNode)).sync().diff(path.toSet) // Get unvisited neighbors

        for (neighbor <- neighbors) {
          queue.enqueue(path :+ neighbor) // Enqueue extended path
        }
      }
    }

    result.toList // BFS ensures paths are returned in shortest-first order
  }

  def reverseEdgesFor(id: Id[To]): Task[Set[Id[From]]] = _edgesReverseStore.transaction { implicit transaction =>
    transaction.get(id.asInstanceOf[Id[EdgeConnections[Doc, To, From]]]).map(_.map(_.to).getOrElse(Set.empty))
  }*/
}
