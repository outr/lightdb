package lightdb.indexeddb

import org.scalajs.dom.{IDBDatabase, IDBFactory, IDBRequest, IDBTransaction, IDBTransactionMode}
import rapid.Task

import scala.scalajs.js

/**
 * Bridges IndexedDB's callback API onto rapid `Task`s.
 *
 * IndexedDB commits a transaction automatically once it has no pending requests and control returns to the event
 * loop. A rapid fiber resumes after each asynchronous step on a new macrotask, so an IndexedDB transaction can never
 * span more than one `Task` step: every request belonging to one IndexedDB transaction must be issued synchronously,
 * inside a single step. The helpers below are shaped around that rule.
 */
private[indexeddb] object IDB {
  /** Documents live in one object store per database, keyed by the document id (out-of-line keys). */
  val ObjectStore: String = "docs"

  /** The browser's `indexedDB`, or the one installed globally by a test shim such as fake-indexeddb in Node. */
  def factory: IDBFactory = {
    val f = js.Dynamic.global.indexedDB
    if (js.isUndefined(f)) throw new UnsupportedOperationException("IndexedDB is not available in this environment")
    f.asInstanceOf[IDBFactory]
  }

  def open(name: String): Task[IDBDatabase] = Task.defer {
    val completable = Task.completable[IDBDatabase]
    val request = factory.open(name, 1)
    request.onupgradeneeded = _ => {
      val db = request.result
      if (!db.objectStoreNames.contains(ObjectStore)) db.createObjectStore(ObjectStore)
    }
    request.onsuccess = _ => completable.success(request.result)
    request.onerror = _ => completable.failure(error(s"open $name", request.error))
    request.onblocked = _ => completable.failure(new IllegalStateException(s"Opening IndexedDB database $name is blocked by another connection"))
    completable
  }

  def delete(name: String): Task[Unit] = Task.defer {
    val completable = Task.completable[Unit]
    val request = factory.deleteDatabase(name)
    request.onsuccess = _ => completable.success(())
    request.onerror = _ => completable.failure(error(s"delete $name", request.error))
    completable
  }

  /** A single read in its own read-only transaction. */
  def read[A](db: IDBDatabase)(f: org.scalajs.dom.IDBObjectStore => IDBRequest[?, A]): Task[A] = Task.defer {
    val completable = Task.completable[A]
    val request = f(db.transaction(ObjectStore, IDBTransactionMode.readonly).objectStore(ObjectStore))
    request.onsuccess = _ => completable.success(request.result)
    request.onerror = _ => completable.failure(error("read", request.error))
    completable
  }

  /**
   * Issue every write in `f` synchronously in one read-write transaction and complete once it has committed.
   * Completion is taken from the transaction's `complete` event, not from the individual requests, so the task
   * succeeds only when the writes are durable and fails if IndexedDB aborts any of them.
   */
  def write(db: IDBDatabase)(f: org.scalajs.dom.IDBObjectStore => Unit): Task[Unit] = Task.defer {
    val completable = Task.completable[Unit]
    val tx: IDBTransaction = db.transaction(ObjectStore, IDBTransactionMode.readwrite)
    tx.oncomplete = _ => completable.success(())
    tx.onabort = _ => completable.failure(error("write (aborted)", tx.error))
    tx.onerror = _ => completable.failure(error("write", tx.error))
    try f(tx.objectStore(ObjectStore))
    catch {
      case t: Throwable =>
        tx.abort()
        completable.failure(t)
    }
    completable
  }

  private def error(operation: String, cause: js.Any): Throwable = {
    val message = if (cause == null || js.isUndefined(cause)) "unknown error" else cause.asInstanceOf[js.Dynamic].message.toString
    new RuntimeException(s"IndexedDB $operation failed: $message")
  }
}
