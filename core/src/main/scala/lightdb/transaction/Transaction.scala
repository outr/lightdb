package lightdb.transaction

import fabric._
import fabric.rw._
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.field.Field.UniqueIndex
import lightdb.graph.EdgeModel
import lightdb.store.Store
import lightdb.traversal.{GraphStep, GraphTraversalEngine}
import rapid._

trait Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def store: Store[Doc, Model]

  final def insert(doc: Doc): Task[Doc] = store.trigger.insert(doc)(this).flatMap { _ =>
    _insert(doc)
  }
  def insert(docs: Seq[Doc]): Task[Seq[Doc]] = docs.map(insert).tasks
  final def upsert(doc: Doc): Task[Doc] = store.trigger.upsert(doc)(this).flatMap { _ =>
    _upsert(doc)
  }
  def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = docs.map(upsert).tasks
  final def exists(id: Id[Doc]): Task[Boolean] = _exists(id)
  final def count: Task[Int] = _count
  final def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = {
    val (field, value) = f(store.model)
    store.trigger.delete(field, value)(this).flatMap(_ => _delete(field, value))
  }
  final def commit: Task[Unit] = _commit
  final def rollback: Task[Unit] = _rollback
  final def close: Task[Unit] = _close

  def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = {
    val (field, value) = f(store.model)
    _get(field, value)
  }
  def get(id: Id[Doc]): Task[Option[Doc]] = _get(store.idField, id)
  def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream
    .emits(ids)
    .evalMap(apply)
  def apply(id: Id[Doc]): Task[Doc] = get(id).map(_.getOrElse {
    throw DocNotFoundException(store.name, "_id", id)
  })
  def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = get[V](f).map {
    case Some(doc) => doc
    case None =>
      val (field, value) = f(store.model)
      throw DocNotFoundException(store.name, field.name, value)
  }
  def getOrCreate(id: Id[Doc], create: => Doc, establishLock: Boolean = true): Task[Doc] =
    modify(id, establishLock = establishLock) {
      case Some(doc) => Task.pure(Some(doc))
      case None => Task.pure(Some(create))
    }.map(_.get)
  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = store.lock(id, get(id), establishLock) { existing =>
    f(existing).flatMap {
      case Some(doc) => upsert(doc).map(_ => Some(doc))
      case None if deleteOnNone => delete(id).map(_ => None)
      case None => Task.pure(None)
    }
  }
  def delete(id: Id[Doc]): Task[Boolean] = delete(_._id -> id)

  def traverse[From <: Document[From], To <: Document[To]](start: Id[From]): GraphTraversalEngine[From, To] =
    traverse(Set(start))

  def traverse[From <: Document[From], To <: Document[To]](starts: Set[Id[From]]): GraphTraversalEngine[From, To] = {
    store.model match {
      case em: EdgeModel[Doc @unchecked, From @unchecked, To @unchecked] @unchecked =>
        val step = new GraphStep[Doc, Model, From, To] {
          override def neighbors(id: Id[From])(implicit t: Transaction[Doc, Model]): Task[Set[Id[To]]] =
            em.edgesFor(id)
        }
        GraphTraversalEngine.start[Doc, Model, From, To](starts, step)(this)
      case _ =>
        throw new UnsupportedOperationException(
          s"traverse(...) is only supported on Store instances with EdgeModel, but got: ${store.model.getClass}"
        )
    }
  }

  def list: Task[List[Doc]] = stream.toList
  def stream: rapid.Stream[Doc] = jsonStream.map(_.as[Doc](store.model.rw))
  def jsonStream: rapid.Stream[Json]
  def truncate: Task[Int]

  protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]]
  protected def _insert(doc: Doc): Task[Doc]
  protected def _upsert(doc: Doc): Task[Doc]
  protected def _exists(id: Id[Doc]): Task[Boolean]
  protected def _count: Task[Int]
  protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean]
  protected def _commit: Task[Unit]
  protected def _rollback: Task[Unit]
  protected def _close: Task[Unit]
}