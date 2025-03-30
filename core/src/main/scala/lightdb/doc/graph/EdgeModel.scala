package lightdb.doc.graph

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import rapid.{Task, logger}

import scala.language.implicitConversions

trait EdgeModel[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends DocumentModel[Doc] {
  implicit def from2Id(id: Id[From]): Id[EdgeConnections[From, To]] = Id(id.value)
  implicit def to2Id(id: Id[To]): Id[EdgeConnections[To, From]] = Id(id.value)

  val _from: UniqueIndex[Doc, Id[From]] = field.unique("_from", _._from)
  val _to: UniqueIndex[Doc, Id[To]] = field.unique("_to", _._to)

  private type D = EdgeConnections[From, To]
  private type M = EdgeConnectionsModel[From, To]
  private type RD = EdgeConnections[To, From]
  private type RM = EdgeConnectionsModel[To, From]

  private var _edgesStore: Store[D, M] = _
  private var _edgesReverseStore: Store[RD, RM] = _

  def edgesStore: Store[D, M] = _edgesStore
  def edgesReverseStore: Store[RD, RM] = _edgesReverseStore

  private val edgesModel: EdgeConnectionsModel[From, To] = EdgeConnectionsModel()
  private val edgesReverseModel: EdgeConnectionsModel[To, From] = EdgeConnectionsModel()

  def edgesFor(id: Id[From]): Task[Set[Id[To]]] = _edgesStore.transaction { implicit transaction =>
    _edgesStore.get(id.asInstanceOf[Id[EdgeConnections[From, To]]]).map(_.map(_.connections).getOrElse(Set.empty))
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
    _edgesReverseStore.get(id.asInstanceOf[Id[EdgeConnections[To, From]]]).map(_.map(_.connections).getOrElse(Set.empty))
  }

  override protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] = {
    super.initialize(store).flatMap { _ =>
      _edgesStore = store.storeManager.create[D, M](
        db = store.lightDB,
        model = edgesModel,
        name = s"${store.name}-edges",
        storeMode = StoreMode.All[D, M]()
      )
      _edgesReverseStore = store.storeManager.create[RD, RM](
        db = store.lightDB,
        model = edgesReverseModel,
        name = s"${store.name}-reverseEdges",
        storeMode = StoreMode.All[RD, RM]()
      )
      // TODO: Validate store contents against store to verify integrity
      store.trigger += new StoreTrigger[Doc] {
        override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = add(doc)

        override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = add(doc)

        override def delete[V](index: Field.UniqueIndex[Doc, V], value: V)
                              (implicit transaction: Transaction[Doc]): Task[Unit] =
          store.get(_ => index -> value).flatMap {
            case Some(doc) => remove(doc)
            case None => Task.unit
          }

        override def dispose(): Task[Unit] = for {
          _ <- _edgesStore.dispose
          _ <- _edgesReverseStore.dispose
        } yield ()

        override def truncate(): Task[Unit] = for {
          _ <- _edgesStore.transaction { implicit transaction =>
            _edgesStore.truncate()
          }
          _ <- _edgesReverseStore.transaction { implicit transaction =>
            _edgesReverseStore.truncate()
          }
        } yield ()
      }

      for {
        _ <- _edgesStore.init
        _ <- _edgesReverseStore.init
      } yield ()
    }
  }

  protected def add(doc: Doc): Task[Unit] = for {
    _ <- _edgesStore.transaction { implicit transaction =>
      _edgesStore.modify(doc._from) { edgeOption =>
        Task {
          Some(edgeOption match {
            case Some(e) => e.copy(
              connections = e.connections + doc._to
            )
            case None => EdgeConnections(
              _id = doc._from,
              connections = Set(doc._to)
            )
          })
        }
      }
    }
    _ <- _edgesReverseStore.transaction { implicit transaction =>
      _edgesReverseStore.modify(doc._to) { edgeReverseOption =>
        Task {
          Some(edgeReverseOption match {
            case Some(e) => e.copy(
              connections = e.connections + doc._from
            )
            case None => EdgeConnections(
              _id = doc._to,
              connections = Set(doc._from)
            )
          })
        }
      }
    }
  } yield ()

  protected def remove(doc: Doc): Task[Unit] = for {
    _ <- _edgesStore.transaction { implicit transaction =>
      _edgesStore.modify(doc._from) { edgeOption =>
        Task {
          edgeOption.flatMap { e =>
            val edge = e.copy(
              connections = e.connections - doc._to
            )
            if (edge.connections.isEmpty) {
              None
            } else {
              Some(edge)
            }
          }
        }
      }
    }
    _ <- _edgesReverseStore.transaction { implicit transaction =>
      _edgesReverseStore.modify(doc._to) { edgeReverseOption =>
        Task {
          edgeReverseOption.flatMap { e =>
            val edge = e.copy(
              connections = e.connections - doc._from
            )
            if (edge.connections.isEmpty) {
              None
            } else {
              None
            }
          }
        }
      }
    }
  } yield ()
}
