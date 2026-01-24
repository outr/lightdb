package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Collection, Store}
import rapid.{Task, logger}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Concurrently applies updates in the transaction. Will block until complete during transaction commit.
 *
 * Note: This will keep all objects in memory while they are being processed. This can lead to out-of-memory errors if
 * updates are going significantly faster than they can be applied.
 *
 * This can lead to great performance improvements, but unexpectedly long commit delays.
 */
case class AsynchronousCachedUpdateHandler[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching],
  maxCache: Int = 5_000,
  monitorDelay: FiniteDuration = 250.millis) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
  @volatile private var keepAlive = true
  private val cached = new AtomicInteger(0)
  private val queue = new ConcurrentLinkedQueue[Task[Unit]]

  Task {
    while keepAlive || !queue.isEmpty do {
      Option(queue.poll()) match {
        case Some(task) =>
          try {
            task.function(cached.decrementAndGet()).sync()
          } catch {
            case t: Throwable => logger.error("Error processing search update", t)
          }
        case None => Task.sleep(monitorDelay).sync()
      }
    }
  }.start()

  private def add(task: Task[Unit]): Task[Unit] = Task.defer {
    if cached.get() >= maxCache then {
      Task.sleep(250.millis).next(add(task))
    } else {
      cached.incrementAndGet()
      queue.add(task)
      Task.unit
    }
  }

  override def insert(doc: Doc): Task[Unit] = add(txn.searching.insert(doc).unit)
  override def upsert(doc: Doc): Task[Unit] = add(txn.searching.upsert(doc).unit)
  override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] =
    add(txn.searching.delete(_ => index -> value).unit)
  override def commit: Task[Unit] = add(txn.searching.commit)
    .condition(Task(cached.get() <= 0))
  override def rollback: Task[Unit] = add(txn.searching.rollback)
  override def truncate: Task[Unit] = add(txn.searching.truncate.unit)
  override def close: Task[Unit] = Task.function {
    keepAlive = false
  }.condition(Task(cached.get() <= 0)).next(txn.searching.commit)
}