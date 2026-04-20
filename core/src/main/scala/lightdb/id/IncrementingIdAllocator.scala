package lightdb.id

import fabric.rw.longRW
import lightdb.StoredValue
import lightdb.doc.Document
import lightdb.store.Store
import rapid.Task

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Allocates monotonically increasing `Long` ids for a document model.
 *
 * Runtime: `nextId` is an in-memory `AtomicLong.incrementAndGet()` — no I/O on the happy path.
 *
 * Persistence: the current counter is flushed to `LightDB.stored` (backed by `backingStore`) debounced —
 * at most once per `flushDelay` — and synchronously whenever the atomic exceeds the last persisted value by
 * more than `burstCap`, bounding the worst-case gap between in-memory and persisted state.
 *
 * Startup: loads the persisted counter and probes forward in the store to find the highest existing id,
 * jumping past any ids allocated before a crash that never got persisted. This preserves strict monotonicity
 * at the cost of a startup probe proportional to the unflushed burst.
 *
 * Deletion caveat: if a recent allocation was deleted between crash and restart, the probe stops at that gap
 * and the id can be reused. For strict never-reuse semantics, avoid deletions of recently-allocated ids
 * during/around a crash.
 */
class IncrementingIdAllocator[Doc <: Document[Doc]](name: String,
                                                    startAt: Long = 1L,
                                                    burstCap: Long = 1000L,
                                                    flushDelay: FiniteDuration = 5.seconds) {
  private val atomic = new AtomicLong(startAt - 1L)
  private val persistedHiWater = new AtomicLong(startAt - 1L)
  private val pendingFlush = new AtomicBoolean(false)
  @volatile private var storedRef: Option[StoredValue[Long]] = None

  /**
   * Load the persisted counter, probe forward to find the highest existing id, and prime the in-memory atomic.
   * Call this from `DocumentModel.init(store)`.
   */
  def initialize(store: Store[Doc, _]): Task[Unit] = Task.defer {
    val stored: StoredValue[Long] = store.lightDB.stored[Long](s"_incrementingId_$name", startAt - 1L)
    storedRef = Some(stored)
    stored.get().flatMap { from =>
      probeForward(store, from).flatMap { highest =>
        atomic.set(highest)
        persistedHiWater.set(highest)
        if highest > from then stored.set(highest).unit else Task.unit
      }
    }
  }

  /** Allocate the next id. Safe for concurrent callers. Throws if called before [[initialize]] completes. */
  def nextId: Task[IncrementingId[Doc]] = Task.defer {
    if storedRef.isEmpty then {
      Task.error(new IllegalStateException(
        s"IncrementingIdAllocator[$name] not initialized — call initialize(store) from DocumentModel.init first."
      ))
    } else {
      val n = atomic.incrementAndGet()
      val afterFlush: Task[Unit] =
        if n - persistedHiWater.get() > burstCap then syncFlush()
        else scheduleDebouncedFlush()
      afterFlush.map(_ => IncrementingId[Doc](n))
    }
  }

  /** Force a synchronous flush of the counter to persistent storage (e.g. on graceful shutdown). */
  def flush(): Task[Unit] = syncFlush()

  private def syncFlush(): Task[Unit] = storedRef match {
    case Some(stored) =>
      val n = atomic.get()
      val previous = persistedHiWater.get()
      if n > previous then {
        stored.set(n).map { _ =>
          persistedHiWater.updateAndGet(p => math.max(p, n))
          ()
        }
      } else Task.unit
    case None => Task.unit
  }

  private def scheduleDebouncedFlush(): Task[Unit] = {
    if pendingFlush.compareAndSet(false, true) then {
      Task.sleep(flushDelay).next {
        Task.defer {
          pendingFlush.set(false)
          syncFlush()
        }
      }.startUnit()
    }
    Task.unit
  }

  private def probeForward(store: Store[Doc, _], from: Long): Task[Long] = store.transaction { tx =>
    def loop(n: Long): Task[Long] = {
      val id = IncrementingId[Doc](n + 1)
      tx.exists(id).flatMap {
        case true => loop(n + 1)
        case false => Task.pure(n)
      }
    }
    loop(from)
  }
}
