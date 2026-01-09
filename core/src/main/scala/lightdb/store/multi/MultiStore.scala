package lightdb.store.multi

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid._

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class MultiStore[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Txn <: Transaction[Doc, Model],
  S <: Store[Doc, Model] { type TX = Txn },
  Key
](stores: Map[Key, S]) {
  def apply(key: Key): S = stores(key)

  def transaction[Return](f: TXN => Task[Return]): Task[Return] = Task.defer {
    val txn = new TXN
    Task.defer(f(txn)).guarantee(txn.release)
  }

  class TXN {
    private val transactions = new ConcurrentHashMap[Key, Txn]

    def apply(key: Key): Txn = transactions.computeIfAbsent(key, _ => {
      val s = stores(key)
      s.transaction.create(None).sync()
    })

    def release: Task[Unit] = transactions
      .entrySet()
      .asScala
      .map { e =>
        val s = stores(e.getKey)
        s.transaction.release(e.getValue)
      }
      .tasks
      .unit
  }
}