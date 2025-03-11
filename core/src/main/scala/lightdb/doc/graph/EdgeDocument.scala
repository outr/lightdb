package lightdb.doc.graph

import fabric.rw._
import lightdb.Id
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.trigger.CollectionTrigger
import rapid.Task

import scala.language.implicitConversions

trait EdgeDocument[Doc <: Document[Doc], From <: Document[From], To <: Document[To]] extends Document[Doc] {
  def _from: Id[From]
  def _to: Id[To]
}

trait EdgeModel[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends DocumentModel[Doc] {
  implicit def from2Id(id: Id[From]): Id[EdgeConnections[From, To]] = Id(id.value)
  implicit def to2Id(id: Id[To]): Id[EdgeConnections[To, From]] = Id(id.value)

  private type D = EdgeConnections[From, To]
  private type M = EdgeConnectionsModel[From, To]
  private type RD = EdgeConnections[To, From]
  private type RM = EdgeConnectionsModel[To, From]

  private var edgesStore: Store[D, M] = _
  private var edgesReverseStore: Store[RD, RM] = _

  private val edgesModel: EdgeConnectionsModel[From, To] = EdgeConnectionsModel()
  private val edgesReverseModel: EdgeConnectionsModel[To, From] = EdgeConnectionsModel()

  def edgesFor(id: Id[From]): Task[Set[Id[To]]] = edgesStore.transaction { implicit transaction =>
    edgesStore.get(edgesModel._id, id.asInstanceOf[Id[EdgeConnections[From, To]]]).map(_.map(_.connections).getOrElse(Set.empty))
  }

  def reverseEdgesFor(id: Id[To]): Task[Set[Id[From]]] = edgesReverseStore.transaction { implicit transaction =>
    edgesReverseStore.get(edgesReverseModel._id, id.asInstanceOf[Id[EdgeConnections[To, From]]]).map(_.map(_.connections).getOrElse(Set.empty))
  }

  override def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Task[Unit] = {
    super.init(collection).flatMap { _ =>
      edgesStore = collection.store.storeManager.create[D, M](
        db = collection.db,
        model = edgesModel,
        name = s"${collection.name}-edges",
        storeMode = StoreMode.All[D, M]()
      )
      edgesReverseStore = collection.store.storeManager.create[RD, RM](
        db = collection.db,
        model = edgesReverseModel,
        name = s"${collection.name}-reverseEdges",
        storeMode = StoreMode.All[RD, RM]()
      )
      collection.trigger += new CollectionTrigger[Doc] {
        override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = add(doc)

        override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = add(doc)

        override def delete[V](index: Field.UniqueIndex[Doc, V], value: V)
                              (implicit transaction: Transaction[Doc]): Task[Unit] =
          collection.get(_ => index -> value).flatMap {
            case Some(doc) => remove(doc)
            case None => Task.unit
          }

        override def dispose(): Task[Unit] = for {
          _ <- edgesStore.dispose
          _ <- edgesReverseStore.dispose
        } yield ()

        override def truncate(): Task[Unit] = for {
          _ <- edgesStore.transaction { implicit transaction =>
            edgesStore.truncate()
          }
          _ <- edgesReverseStore.transaction { implicit transaction =>
            edgesReverseStore.truncate()
          }
        } yield ()
      }

      for {
        _ <- edgesStore.init
        _ <- edgesReverseStore.init
      } yield ()
    }
  }

  protected def add(doc: Doc): Task[Unit] = for {
    _ <- edgesStore.transaction { implicit transaction =>
      edgesStore.modify(doc._from) { edgeOption =>
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
    _ <- edgesReverseStore.transaction { implicit transaction =>
      edgesReverseStore.modify(doc._to) { edgeReverseOption =>
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
    _ <- edgesStore.transaction { implicit transaction =>
      edgesStore.modify(doc._from) { edgeOption =>
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
    _ <- edgesReverseStore.transaction { implicit transaction =>
      edgesReverseStore.modify(doc._to) { edgeReverseOption =>
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

case class EdgeConnections[From <: Document[From], To <: Document[To]](_id: Id[EdgeConnections[From, To]], connections: Set[Id[To]]) extends Document[EdgeConnections[From, To]]

case class EdgeConnectionsModel[From <: Document[From], To <: Document[To]]() extends DocumentModel[EdgeConnections[From, To]] with JsonConversion[EdgeConnections[From, To]] {
  override implicit val rw: RW[EdgeConnections[From, To]] = RW.gen

  val connections: UniqueIndex[EdgeConnections[From, To], Set[Id[To]]] = field.unique("connections", _.connections)
}