package lightdb.store.multi

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.*

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
 * A grouping of N underlying stores keyed by `Key`. Built via `db.store(model).multi(keys)`.
 *
 * Implementation note: the previous `MultiStore[Doc, Model, Txn, S, Key]` shape required callers to
 * supply both the transaction type `Txn` and the store type `S` with a `{ type TX = Txn }` refinement.
 * That made path-dependent invocation from a builder (`sm.S[Doc, Model]#TX`) infeasible — type
 * projection on the abstract `sm.S` is not a "legal path" in Scala 3. Dropping `Txn` and using `S#TX`
 * internally lets the builder thread through `sm.S[Doc, Model]` directly while keeping the same
 * runtime behavior.
 */
class MultiStore[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  S <: Store[Doc, Model],
  Key
](storesMap: Map[Key, S]) { ms =>
  def apply(key: Key): S = storesMap.getOrElse(key, throw new RuntimeException(s"Unable to find $key. Available keys: ${storesMap.keys.mkString(", ")}"))

  def stores: List[S] = storesMap.values.iterator.toList

  lazy val keyStream: rapid.Stream[Key] = rapid.Stream.fromIterator(Task(storesMap.keys.iterator))
  lazy val storesStream: rapid.Stream[S] = rapid.Stream.fromIterator(Task(storesMap.values.iterator))

  def transaction[Return](f: TXN => Task[Return]): Task[Return] = Task.defer {
    val txn = new TXN
    Task.defer(f(txn)).guarantee(txn.release)
  }

  def truncate: Task[Int] = transaction { txn =>
    keyStream
      .evalMap { key =>
        txn(key).truncate
      }
      .fold(0)((total, count) => Task.pure(total + count))
  }

  class TXN {
    // S#TX projection isn't legal when S is abstract, so we hold the base Transaction[Doc, Model]
    // type — sufficient for the typical multi-shard read/write workflow. Backend-specific cast at
    // release-time is unavoidable here because each store's `transaction.release` requires its own
    // path-dependent TX type.
    private val transactions = new ConcurrentHashMap[Key, Transaction[Doc, Model]]

    def apply(key: Key): Transaction[Doc, Model] = transactions.computeIfAbsent(key, _ => {
      val s = ms(key)
      s.transaction.create().sync()
    })

    def release: Task[Unit] = transactions
      .entrySet()
      .asScala
      .map { e =>
        val s = ms(e.getKey)
        s.transaction.release(e.getValue.asInstanceOf[s.TX])
      }
      .tasks
      .unit
  }
}