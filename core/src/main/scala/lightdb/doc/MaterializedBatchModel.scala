package lightdb.doc

import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

trait MaterializedBatchModel[Doc <: Document[Doc], MaterialDoc <: Document[MaterialDoc], MaterialModel <: DocumentModel[MaterialDoc]] extends MaterializedModel[Doc, MaterialDoc, MaterialModel] {
  protected def maxBatchSize: Int = 10000

  private val map = new ConcurrentHashMap[Transaction[MaterialDoc, MaterialModel], TransactionState]

  private def changed(docState: DocState[MaterialDoc])(implicit transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = Task {
    map.compute(transaction, (_, current) => {
      val state = Option(current).getOrElse(new TransactionState)
      state.changed(docState)
      state
    })
    val state = map.get(transaction)
    if (state.size > maxBatchSize) {
      val list = state.process()
      process(list)
    } else {
      Task.unit
    }
  }.flatten

  protected def process(list: List[List[DocState[MaterialDoc]]]): Task[Unit]

  override protected def adding(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] =
    changed(DocState.Added(doc))

  override protected def modifying(oldDoc: MaterialDoc, newDoc: MaterialDoc)
                                  (implicit transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] =
    changed(DocState.Modified(newDoc))

  override protected def removing(doc: MaterialDoc)(implicit transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] =
    changed(DocState.Removed(doc))

  override protected def transactionStart(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = Task.unit

  override protected def transactionEnd(transaction: Transaction[MaterialDoc, MaterialModel]): Task[Unit] = Task {
    Option(map.get(transaction)).foreach { state =>
      if (state.size > 0) {
        val list = state.process()
        process(list).sync()
      }
      map.remove(transaction)
    }
  }

  private class TransactionState {
    private val map = new ConcurrentHashMap[Id[MaterialDoc], List[DocState[MaterialDoc]]]
    private val counter = new AtomicInteger(0)

    def changed(state: DocState[MaterialDoc]): Unit = {
      map.compute(state.doc._id, (_, current) => {
        val list = Option(current).getOrElse(Nil)
        state :: list
      })
      counter.incrementAndGet()
    }

    def size: Int = counter.get()

    def process(): List[List[DocState[MaterialDoc]]] = synchronized {
      try {
        map.values().asScala.toList
      } finally {
        map.clear()
        counter.set(0)
      }
    }
  }
}
