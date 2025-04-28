package lightdb.transaction

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.feature.FeatureSupport
import lightdb.field.Field.UniqueIndex
import lightdb.store.Store
import rapid._

/*final class Transaction[Doc <: Document[Doc]] extends FeatureSupport[TransactionKey] { transaction =>
  def commit(): Task[Unit] = features.map {
    case f: TransactionFeature => f.commit()
    case _ => Task.unit // Ignore
  }.tasks.unit

  def rollback(): Task[Unit] = features.map {
    case f: TransactionFeature => f.rollback()
    case _ => Task.unit // Ignore
  }.tasks.unit

  def close(): Task[Unit] = features.map {
    case f: TransactionFeature => f.close()
    case _ => Task.unit // Ignore
  }.tasks.unit
}*/

trait Transaction[Doc <: Document[Doc], +Model <: DocumentModel[Doc]] {
  protected def store: Store[Doc, _ <: Model]

  final def get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = ???
  final def insert(doc: Doc): Task[Doc] = store.trigger.insert(doc)(this).flatMap { _ =>
    _insert(doc)
  }
  def insert(docs: Seq[Doc]): Task[Seq[Doc]] = for {
    _ <- docs.map(store.trigger.insert(_)(this)).tasks
    _ <- docs.map(insert).tasks
  } yield docs
  final def upsert(doc: Doc): Task[Doc] = store.trigger.upsert(doc)(this).flatMap { _ =>
    _upsert(doc)
  }
  final def exists(id: Id[Doc]): Task[Boolean] = _exists(id)
  final def count: Task[Int] = ???
  final def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = ???

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
  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = ???

  protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]]
  protected def _insert(doc: Doc): Task[Doc]
  protected def _insert(docs: Seq[Doc]): Task[Seq[Doc]]
  protected def _upsert(doc: Doc): Task[Doc]
  protected def _exists(id: Id[Doc]): Task[Boolean]
  protected def _count: Task[Int]
  protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean]
}