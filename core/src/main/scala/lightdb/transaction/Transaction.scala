package lightdb.transaction

import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.Store
import lightdb.transaction.batch.{Batch, DirectBatch}
import rapid.*

trait Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def store: Store[Doc, Model]
  def parent: Option[Transaction[Doc, Model]]

  final def insert(doc: Doc): Task[Doc] = store.trigger.insert(doc, this).next(_insert(doc))
  def insert(stream: rapid.Stream[Doc]): Task[Int] = stream.evalMap(insert).count
  final def insert(docs: Seq[Doc]): Task[Seq[Doc]] = insert(rapid.Stream.emits(docs)).map(_ => docs)
  def insertJson(stream: rapid.Stream[Json]): Task[Int] = insert(stream.map(_.as[Doc](store.model.rw)))

  final def upsert(doc: Doc): Task[Doc] = store.trigger.upsert(doc, this).next(_upsert(doc))
  def upsert(stream: rapid.Stream[Doc]): Task[Int] = stream.evalMap(upsert).count
  final def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = upsert(rapid.Stream.emits(docs)).map(_ => docs)

  final def exists(id: Id[Doc]): Task[Boolean] = _exists(id)
  final def count: Task[Int] = _count

  /**
   * Gets an estimated count if supported by database. Falls back to using count.
   */
  def estimatedCount: Task[Int] = count
  final def commit: Task[Unit] = _commit
  final def rollback: Task[Unit] = _rollback
  final def close: Task[Unit] = batch.closeAll.next(_close)

  def get(id: Id[Doc]): Task[Option[Doc]] = _get(store.idField, id)
  def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream
    .emits(ids)
    .evalMap(apply)
  def apply(id: Id[Doc]): Task[Doc] = get(id).map(_.getOrElse {
    throw DocNotFoundException(store.name, "_id", id)
  })
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
  final def delete(id: Id[Doc]): Task[Boolean] = store.trigger.delete(id, this).next(_delete(id))

  def list: Task[List[Doc]] = stream.toList
  def stream: rapid.Stream[Doc] = jsonStream.map(_.as[Doc](store.model.rw))
  def jsonStream: rapid.Stream[Json]
  def truncate: Task[Int]

  private var batches = Set.empty[Batch[Doc, Model]]

  protected def createBatch(): Task[Batch[Doc, Model]] = Task.pure(DirectBatch(this))
  protected def releaseBatch(batch: Batch[Doc, Model]): Task[Unit] = Task {
    batch.synchronized {
      batches -= batch
    }
  }

  object batch { b =>
    def active: Task[Int] = Task.function(batches.size)

    def create: Task[Batch[Doc, Model]] = createBatch().flatTap { batch =>
      Task {
        b.synchronized {
          batches += batch
        }
      }
    }

    def closeAll: Task[Unit] = if (batches.nonEmpty) {
      batches.toSeq.map(_.close).tasks.unit
    } else {
      Task.unit
    }
  }

  protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]]
  protected def _insert(doc: Doc): Task[Doc]
  protected def _upsert(doc: Doc): Task[Doc]
  protected def _delete(id: Id[Doc]): Task[Boolean]
  protected def _exists(id: Id[Doc]): Task[Boolean]
  protected def _count: Task[Int]
  protected def _commit: Task[Unit]
  protected def _rollback: Task[Unit]
  protected def _close: Task[Unit]

  protected def toString(doc: Doc): String = JsonFormatter.Compact(doc.json(store.model.rw))
  protected def fromString(string: String): Doc = toJson(string).as[Doc](store.model.rw)
  protected def toJson(string: String): Json = JsonParser(string)
}

object Transaction {
  def releaseBatch[Doc <: Document[Doc], Model <: DocumentModel[Doc]](transaction: Transaction[Doc, Model],
                                                                      batch: Batch[Doc, Model]): Task[Unit] =
    transaction.releaseBatch(batch)
}