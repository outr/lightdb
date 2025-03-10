package lightdb.doc.graph

import fabric.rw.RW
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
  private type D = EdgeConnections[From, To]
  private type M = EdgeConnectionsModel[From, To]
  private type RD = EdgeConnections[To, From]
  private type RM = EdgeConnectionsModel[To, From]

  private var edgesStore: Store[D, M] = _
  private var edgesReverseStore: Store[RD, RM] = _

  private val edgesModel: EdgeConnectionsModel[From, To] = EdgeConnectionsModel()
  private val edgesReverseModel: EdgeConnectionsModel[To, From] = EdgeConnectionsModel()

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
        override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = ???

        override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = ???

        override def delete[V](index: Field.UniqueIndex[Doc, V], value: V)
                              (implicit transaction: Transaction[Doc]): Task[Unit] = ???

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

  implicit def from2Id(id: Id[From]): Id[EdgeConnections[From, To]] = Id(id.value)
  implicit def to2Id(id: Id[To]): Id[EdgeConnections[To, From]] = Id(id.value)

  protected def add(doc: Doc): Task[Unit] = for {
    edgeOption <- edgesStore.transaction { implicit transaction =>
      edgesStore.get(edgesModel.from, doc._from)
    }
    edgeReverseOption <- edgesReverseStore.transaction { implicit transaction =>
      edgesReverseStore.get(edgesReverseModel.from, doc._to)
    }
  } yield ()
}

case class EdgeConnections[From <: Document[From], To <: Document[To]](from: Id[From], to: List[Id[To]], _id: Id[EdgeConnections[From, To]]) extends Document[EdgeConnections[From, To]]

case class EdgeConnectionsModel[From <: Document[From], To <: Document[To]]() extends DocumentModel[EdgeConnections[From, To]] with JsonConversion[EdgeConnections[From, To]] {
  override implicit val rw: RW[EdgeConnections[From, To]] = RW.gen

  val from: UniqueIndex[EdgeConnections[From, To], Id[From]] = field.unique("from", _.from)
  val to: UniqueIndex[EdgeConnections[From, To], List[Id[To]]] = field.unique("to", _.to)
}