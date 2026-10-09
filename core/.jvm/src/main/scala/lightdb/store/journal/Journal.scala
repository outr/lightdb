package lightdb.store.journal

import fabric.io.JsonFormatter
import fabric.rw.*
import lightdb.{LightDB, LightDBPlatform}
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.{DuplicateIdException, StoreFailedException}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.store.write.WriteOp
import lightdb.util.Disposable
import rapid.*

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.jdk.CollectionConverters.*

/**
 * The commit-intent journal of a [[JournalStoreManager]]: what makes the wrapped manager's stores joinable.
 *
 * A commit of a group (an owner and the transactions joined to it) writes one intent record holding every op of the
 * group to an intent store on the wrapped manager, in one atomic single-record write. That write is the commit point.
 * The intent then goes to one serial applier, which applies the intents in sequence order to the wrapped stores and
 * deletes each only after it is applied (and so after every earlier one). At open every intent still present is
 * replayed in sequence order before any read, so a commit whose apply a crash interrupted is completed, and two
 * commits to one id end with the later commit's value.
 *
 * Once the intent is durable the commit never fails: an apply error is retried in process with backoff
 * ([[JournalRetry]]); if the retries run out the journal is marked failed, every read of its stores fails loud, and
 * new commits are refused, until a reopen replays the intents. Ops are applied raw (no store triggers fire), so
 * every derived write is applied exactly once: it is already in the intent.
 *
 * The wrapped manager must provide an atomic, durable single-record write of [[maxRecordBytes]] (the intent) and
 * idempotent put and delete by id (an intent may be applied twice: before a crash and at replay). The journal gives
 * atomic durability, not atomic visibility: while an intent is being applied, another reader of the wrapped stores
 * can see some of its rows before the rest.
 */
final class Journal(val wrapped: StoreManager, val name: String, val maxRecordBytes: Long, val retry: JournalRetry) {
  require(maxRecordBytes > 0, s"maxRecordBytes must be positive, not $maxRecordBytes")

  private val stores = new ConcurrentHashMap[String, JournalBacked[?, ?]]()
  private val users = new AtomicInteger(0)
  @volatile private var intentStore: Store[JournalIntent, JournalIntent.type] = null
  private val lock = new ReentrantLock()
  private var nextSequence = 1L
  // The latest pending (committed, not yet applied) state of each (store, id): present after a put, absent after a delete.
  private val pendingRows = new ConcurrentHashMap[(String, String), (Boolean, Long)]()
  private val pendingTruncates = new ConcurrentHashMap[String, Long]()
  @volatile private var _failure: Option[Throwable] = None
  private val queue = new LinkedBlockingQueue[Journal.Work]()
  @volatile private var applier: Thread = null
  @volatile private var disposed = false

  /** The fault that stopped the applier, if one did: reads fail and commits are refused until a reopen. */
  def failure: Option[Throwable] = _failure

  /** How many committed intents wait to be applied. */
  def pending: Int = queue.size()

  private[journal] def register(store: JournalBacked[?, ?]): Unit = {
    if stores.putIfAbsent(store.name, store) != null then
      throw new IllegalArgumentException(s"Journal $name already has a store named ${store.name}")
    users.incrementAndGet()
    synchronized {
      if intentStore == null then {
        val db = store.lightDB
        intentStore = wrapped.create[JournalIntent, JournalIntent.type](db, JournalIntent, name,
          LightDBPlatform.storePath(db.directory, name), StoreMode.All())
      }
    }
  }

  /** How many intent records the intent store holds: committed and not yet applied (or applied, not yet deleted). */
  def unfinishedIntents: Task[Int] = Task.defer(intentStore.transaction(_.count))

  private[journal] def checkReadable(storeName: String): Unit = _failure.foreach { t =>
    throw StoreFailedException(storeName, s"journal $name could not apply a committed intent", t)
  }

  /** Opens the intent store and replays every unfinished intent, in sequence order, before any read. */
  private[journal] lazy val open: Task[Unit] = Task.defer {
    intentStore.init.next(intentStore.transaction(_.stream.toList)).flatMap { intents =>
      val ordered = intents.sortBy(_.sequence)
      if ordered.nonEmpty then scribe.warn(s"Journal $name: replaying ${ordered.size} unfinished commit intent(s)")
      ordered.foldLeft(Task.unit)((task, intent) => task.next(Task {
        applyWithRetry(intent).foreach(t => throw new RuntimeException(s"Journal $name could not replay intent ${intent.sequence}", t))
      })).map { _ =>
        lock.lock()
        try nextSequence = ordered.lastOption.map(_.sequence + 1).getOrElse(1L)
        finally lock.unlock()
        startApplier()
      }
    }
  }.singleton

  /** Commits a group: refuses duplicate inserts, writes the intent (the commit point), then waits for the applier.
    * Fails only before the intent is durable. */
  private[journal] def commit(entries: List[JournalEntry[?, ?]]): Task[Unit] = Task.defer {
    val nonEmpty = entries.filterNot(_.isEmpty)
    if nonEmpty.isEmpty then Task.unit
    else Task {
      lock.lock()
      val work = try {
        _failure.foreach(t => throw StoreFailedException(nonEmpty.head.store.name, s"journal $name has failed; commits are refused until a reopen", t))
        if disposed then throw new IllegalStateException(s"Journal $name is disposed")
        nonEmpty.foreach(refuseDuplicates(_))
        val ops = nonEmpty.flatMap(toOps(_))
        val sequence = nextSequence
        val intent = JournalIntent(ops, JournalIntent.idFor(sequence))
        val bytes = JsonFormatter.Compact(intent.json(JournalIntent.rw)).getBytes(StandardCharsets.UTF_8).length.toLong
        if bytes > maxRecordBytes then
          throw new IllegalStateException(s"Journal $name: the commit's intent is $bytes bytes, past the $maxRecordBytes bytes its store writes atomically")
        Journal.report(JournalStep.BeforeIntent(sequence))
        intentStore.transaction.withoutGroupCommit(_.insert(intent)).sync()
        // The commit point has passed: nothing below may fail the commit.
        nextSequence = sequence + 1
        ops.foreach { op =>
          op.kind match {
            case JournalOpKind.Put => pendingRows.put(op.store -> op.id.get, (true, sequence))
            case JournalOpKind.Delete => pendingRows.put(op.store -> op.id.get, (false, sequence))
            case JournalOpKind.Truncate => pendingTruncates.put(op.store, sequence)
          }
        }
        safely(Journal.report(JournalStep.AfterIntent(sequence)))
        val work = Journal.Work(intent, new CompletableFuture[Unit]())
        queue.put(work)
        work
      } finally lock.unlock()
      awaitQuietly(work.done)
    }
  }

  private def refuseDuplicates[Doc <: Document[Doc], Model <: DocumentModel[Doc]](entry: JournalEntry[Doc, Model]): Unit = {
    val inserts = entry.ops.collect { case WriteOp.Insert(doc) => doc._id }
    if inserts.nonEmpty then {
      val storeName = entry.store.name
      val truncatedPending = pendingTruncates.containsKey(storeName)
      val unknown = inserts.filter { id =>
        Option(pendingRows.get(storeName -> id.value)) match {
          case Some((present, _)) =>
            if present then throw DuplicateIdException(storeName, id)
            false
          case None => !entry.truncate && !truncatedPending
        }
      }
      if unknown.nonEmpty then {
        val u = entry.store.underlying
        val existing = u.transaction.withoutGroupCommit(tx => unknown.map(id => tx.exists(id).map(id -> _)).tasks).sync()
        existing.collectFirst { case (id, true) => id }.foreach(id => throw DuplicateIdException(storeName, id))
      }
    }
  }

  private def toOps[Doc <: Document[Doc], Model <: DocumentModel[Doc]](entry: JournalEntry[Doc, Model]): List[JournalOp] = {
    val storeName = entry.store.name
    val rw = entry.store.model.rw
    val truncate = if entry.truncate then List(JournalOp(storeName, JournalOpKind.Truncate, None, None)) else Nil
    truncate ::: entry.ops.map {
      case WriteOp.Insert(doc) => JournalOp(storeName, JournalOpKind.Put, Some(doc._id.value), Some(doc.json(rw)))
      case WriteOp.Upsert(doc) => JournalOp(storeName, JournalOpKind.Put, Some(doc._id.value), Some(doc.json(rw)))
      case WriteOp.Delete(id) => JournalOp(storeName, JournalOpKind.Delete, Some(id.value), None)
    }
  }

  // -- The serial applier ---------------------------------------------------------------------------------------

  private def startApplier(): Unit = synchronized {
    if applier == null && !disposed then {
      val t = new Thread(() => run(), s"lightdb-journal-$name")
      t.setDaemon(true)
      applier = t
      t.start()
    }
  }

  private def run(): Unit = {
    var running = true
    while running do {
      val work = queue.take()
      if work eq Journal.Stop then running = false
      else {
        try {
          if _failure.isEmpty then {
            applyWithRetry(work.intent) match {
              case None => clearPending(work.intent.sequence)
              case Some(t) =>
                scribe.error(s"Journal $name: intent ${work.intent.sequence} could not be applied after ${retry.attempts} attempts; " +
                  "its stores refuse reads until the database is reopened, when the intent is replayed", t)
                _failure = Some(t)
            }
          }
        } catch {
          case t: Throwable =>
            scribe.error(s"Journal $name: applier fault on intent ${work.intent.sequence}", t)
            _failure = Some(t)
        } finally work.done.complete(())
      }
    }
  }

  private def clearPending(sequence: Long): Unit = {
    pendingRows.entrySet().removeIf(e => e.getValue._2 == sequence)
    pendingTruncates.entrySet().removeIf(e => e.getValue == sequence)
  }

  /** Applies the intent, then deletes it, retrying both. Returns the last error when every attempt failed. */
  private def applyWithRetry(intent: JournalIntent): Option[Throwable] = {
    var attempt = 1
    var result: Option[Throwable] = Some(new IllegalStateException("not attempted"))
    while result.nonEmpty && attempt <= retry.attempts do {
      result = try {
        Journal.report(JournalStep.BeforeApply(intent.sequence, attempt))
        applyIntent(intent)
        Journal.report(JournalStep.BeforeDelete(intent.sequence))
        intentStore.transaction.withoutGroupCommit(_.delete(intent._id)).sync()
        Journal.report(JournalStep.AfterDelete(intent.sequence))
        None
      } catch {
        case t: Throwable =>
          if attempt < retry.attempts then {
            scribe.warn(s"Journal $name: applying intent ${intent.sequence} failed (attempt $attempt of ${retry.attempts}): ${t.getMessage}")
            Thread.sleep(retry.delay(attempt).toMillis)
          }
          Some(t)
      }
      attempt += 1
    }
    result
  }

  private def applyIntent(intent: JournalIntent): Unit = {
    val byStore = intent.ops.groupBy(_.store)
    val order = intent.ops.map(_.store).distinct
    order.foreach { storeName =>
      val store = Option(stores.get(storeName)).getOrElse(throw new IllegalStateException(
        s"Journal $name: intent ${intent.sequence} writes store '$storeName', which is not registered with this database; " +
          "register every journalled store before the database is initialized"))
      applyTo(store, byStore(storeName))
      Journal.report(JournalStep.AfterApplyStore(intent.sequence, storeName))
    }
  }

  // Raw ops: no store trigger fires, so a derived write already in the intent is not written twice.
  private def applyTo[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: JournalBacked[Doc, Model], ops: List[JournalOp]): Unit = {
    val u = store.underlying
    u.init.sync()
    val truncate = ops.exists(_.kind == JournalOpKind.Truncate)
    val writes: List[WriteOp[Doc]] = ops.collect {
      case JournalOp(_, JournalOpKind.Put, _, Some(json)) => WriteOp.Upsert(json.as[Doc](store.model.rw))
      case JournalOp(_, JournalOpKind.Delete, Some(id), _) => WriteOp.Delete(Id[Doc](id))
    }
    u.transaction.withoutGroupCommit { tx =>
      (if truncate then tx.truncate.unit else Task.unit).next(tx.applyWriteOps(writes))
    }.sync()
  }

  private def safely(f: => Unit): Unit =
    try f catch { case t: Throwable => scribe.warn(s"Journal $name: ignoring an error after the commit point: ${t.getMessage}") }

  private def awaitQuietly(future: CompletableFuture[Unit]): Unit = {
    var done = false
    while !done do {
      try {
        future.get()
        done = true
      } catch {
        case _: InterruptedException => ()
        case _: Throwable => done = true
      }
    }
  }

  /** A journalled store was disposed; the last one stops the applier and closes the intent store. */
  private[journal] def release(store: JournalBacked[?, ?]): Task[Unit] = Task.defer {
    stores.remove(store.name, store)
    if users.decrementAndGet() <= 0 then dispose else Task.unit
  }

  lazy val dispose: Task[Unit] = Task.defer {
    disposed = true
    val t = applier
    if t != null then {
      queue.put(Journal.Stop)
      t.join(30_000)
    }
    Option(intentStore).map(_.dispose).getOrElse(Task.unit)
  }.singleton
}

object Journal {
  private[journal] case class Work(intent: JournalIntent, done: CompletableFuture[Unit])

  private[journal] val Stop: Work = Work(JournalIntent(Nil, JournalIntent.idFor(0L)), new CompletableFuture[Unit]())

  /** Called at each step of a journalled commit, on the thread running it. For fault-injection tests: a hook that
    * halts the process simulates a crash at that step, one that throws an apply error. */
  @volatile var faultHook: JournalStep => Unit = _ => ()

  private[journal] def report(step: JournalStep): Unit = faultHook(step)
}
