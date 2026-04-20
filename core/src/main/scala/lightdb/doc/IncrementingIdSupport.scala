package lightdb.doc

import lightdb.id.{Id, IncrementingId, IncrementingIdAllocator}
import lightdb.store.Store
import rapid.Task

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Mix-in trait that adds monotonically increasing `IncrementingId` allocation for a document model.
 *
 * Usage — the `_id` field has no default; use [[newId]] (or the convenience [[newDoc]]) to allocate:
 * {{{
 *   case class Person(name: String, _id: Id[Person]) extends Document[Person]
 *
 *   object Person extends DocumentModel[Person] with IncrementingIdSupport[Person] with JsonConversion[Person] {
 *     override implicit val rw: RW[Person] = RW.gen
 *     val name: I[String] = field.index(_.name)
 *   }
 *
 *   // explicit allocation
 *   for { id <- Person.newId; _ <- tx.insert(Person("Alice", id)) } yield ()
 *
 *   // or via newDoc + insert
 *   Person.newDoc(id => Person("Alice", id)).flatMap(tx.insert)
 * }}}
 *
 * The default `_id = Model.id()` case-class pattern is intentionally NOT supported here — fabric 1.24+ evaluates
 * case-class default args eagerly on every deserialization, which would bump the counter on every read.
 *
 * See [[IncrementingIdAllocator]] for the runtime/persistence/startup-probe behavior.
 */
trait IncrementingIdSupport[Doc <: Document[Doc]] extends DocumentModel[Doc] {
  /** First id to hand out on a fresh database. */
  protected def incrementingIdStartAt: Long = 1L

  /** Force a synchronous flush to the backing store every N allocations (bounds crash-recovery probe cost). */
  protected def incrementingIdBurstCap: Long = 1000L

  /** Debounced async flush delay. */
  protected def incrementingIdFlushDelay: FiniteDuration = 5.seconds

  /** Access to the underlying allocator (for advanced use — e.g. graceful `flush()` on shutdown). */
  private lazy val idAllocator: IncrementingIdAllocator[Doc] = new IncrementingIdAllocator[Doc](
    name = modelName,
    startAt = incrementingIdStartAt,
    burstCap = incrementingIdBurstCap,
    flushDelay = incrementingIdFlushDelay
  )

  override def id(value: String): Id[Doc] = IncrementingId[Doc](value.toLong)
  def id(): Id[Doc] = newId()

  /** Allocate the next id. Runtime is a single in-memory atomic increment on the happy path. */
  def newId: Task[IncrementingId[Doc]] = idAllocator.nextId

  override protected def init[Model <: DocumentModel[Doc]](store: Store[Doc, Model]): Task[Unit] =
    super.init(store).next(idAllocator.initialize(store.asInstanceOf[Store[Doc, _]]))
}
