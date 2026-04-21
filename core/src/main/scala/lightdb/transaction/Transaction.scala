package lightdb.transaction

import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.Store
import lightdb.store.write.WriteOp
import rapid.*

import java.util.concurrent.ConcurrentHashMap

trait Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def store: Store[Doc, Model]
  def parent: Option[Transaction[Doc, Model]]
  def writeHandler: WriteHandler[Doc, Model]

  // -- Tx-local cache overlay -----------------------------------------------------------------------------
  // Writes in this transaction are accumulated here and applied to `store.cache` on commit. On rollback
  // the overlay is discarded, so rolled-back writes never reach the store cache.
  //
  // Values:
  //   Some(doc) — the doc was inserted or upserted
  //   None      — the id was deleted (tombstone)
  //
  // Populated via `applyWriteOps` (below). No-op when `store.cache.isEmpty`.
  private[lightdb] lazy val cachePending: ConcurrentHashMap[Id[Doc], Option[Doc]] =
    new ConcurrentHashMap[Id[Doc], Option[Doc]]()

  final def insert(doc: Doc): Task[Doc] = store.trigger.insert(doc, this)
    .next(writeHandler.write(WriteOp.Insert(doc)))
    .map(_ => doc)
  def insert(stream: rapid.Stream[Doc]): Task[Int] =
    stream.evalMap(insert).count.flatTap(_ => flush)

  final def insert(docs: Seq[Doc]): Task[Seq[Doc]] = insert(rapid.Stream.emits(docs)).map(_ => docs)
  def insertJson(stream: rapid.Stream[Json]): Task[Int] = insert(stream.map(_.as[Doc](store.model.rw)))

  final def upsert(doc: Doc): Task[Doc] = store.trigger.upsert(doc, this)
    .next(writeHandler.write(WriteOp.Upsert(doc)))
    .map(_ => doc)
  def upsert(stream: rapid.Stream[Doc]): Task[Int] =
    stream.evalMap(upsert).count.flatTap(_ => flush)
  final def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = upsert(rapid.Stream.emits(docs)).map(_ => docs)

  final def exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)
  final def count: Task[Int] = _count

  /**
   * Gets an estimated count if supported by database. Falls back to using count.
   */
  def estimatedCount: Task[Int] = count
  def flush: Task[Unit] = writeHandler.flush
  final def commit: Task[Unit] = writeHandler.flush.next(_commit).next(applyCachePending)
  final def close: Task[Unit] = writeHandler.close.next(_close)

  /**
   * Drain `cachePending` into the store-level cache. Called after `_commit` succeeds so rolled-back or
   * failed transactions never touch the cache. No-op when caching is disabled.
   */
  private def applyCachePending: Task[Unit] = Task {
    store.cache.foreach { cache =>
      if (!cachePending.isEmpty) {
        val it = cachePending.entrySet().iterator()
        while (it.hasNext) {
          val e = it.next()
          e.getValue match {
            case Some(doc) => cache.put(e.getKey, doc)
            case None => cache.evict(e.getKey)
          }
        }
        cachePending.clear()
      }
    }
  }

  def get(id: Id[Doc]): Task[Option[Doc]] = writeHandler.get(id).flatMap {
    case Some(result) => Task.pure(result)
    case None =>
      // Tx-local overlay shadows the store cache for in-flight writes. This protects
      // DirectWriteHandler backends from returning stale cached values after a same-tx upsert/delete
      // (the backend already has the new value, but the store cache hasn't been refreshed yet — that
      // happens on commit). For BufferedWriteHandler backends `writeHandler.get` already covered this
      // above, so the overlay is empty for the same id and we fall straight through.
      val pending = cachePending.get(id)
      if (pending != null) Task.pure(pending)  // Some(doc) = pending insert/upsert; None = pending delete
      else store.cache match {
        case None => _get(store.idField, id)
        case Some(cache) => cache.get(id) match {
          case Some(doc) => Task.pure(Some(doc))
          case None => _get(store.idField, id).map { resultOpt =>
            // Read-through: populate the cache on a backend hit. Misses are not cached.
            resultOpt.foreach(doc => cache.put(id, doc))
            resultOpt
          }
        }
      }
  }
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
  final def delete(id: Id[Doc]): Task[Boolean] = store.trigger.delete(id, this)
    .next(writeHandler.write(WriteOp.Delete(id)))
    .map(_ => true)

  def list: Task[List[Doc]] = stream.toList
  def stream: rapid.Stream[Doc] = jsonStream.map(_.as[Doc](store.model.rw))
  def jsonStream: rapid.Stream[Json]
  def truncate: Task[Int]

  private[lightdb] def applyWriteOps(ops: Seq[WriteOp[Doc]]): Task[Unit] =
    ops.map {
      case WriteOp.Insert(doc) => _insert(doc).map(_ => trackCacheWrite(doc._id, Some(doc)))
      case WriteOp.Upsert(doc) => _upsert(doc).map(_ => trackCacheWrite(doc._id, Some(doc)))
      case WriteOp.Delete(id) => _delete(id).map(_ => trackCacheWrite(id, None))
    }.tasks.unit

  /**
   * Record a pending cache mutation for this transaction. The operation is buffered in [[cachePending]]
   * and applied to the store-level cache on commit (or discarded on rollback). No-op when caching is
   * disabled.
   */
  private def trackCacheWrite(id: Id[Doc], doc: Option[Doc]): Unit =
    if (store.cache.isDefined) cachePending.put(id, doc)

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