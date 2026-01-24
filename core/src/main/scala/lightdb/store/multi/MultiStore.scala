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
    private val transactions = new ConcurrentHashMap[Key, Txn]

    def apply(key: Key): Txn = transactions.computeIfAbsent(key, _ => {
      val s = ms(key)
      s.transaction.create().sync()
    })

    def release: Task[Unit] = transactions
      .entrySet()
      .asScala
      .map { e =>
        val s = ms(e.getKey)
        s.transaction.release(e.getValue)
      }
      .tasks
      .unit
  }
}