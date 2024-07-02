package lightdb.collection

import lightdb.doc.DocModel
import lightdb.{Query, Store, Transaction}

case class Collection[Doc, Model <: DocModel[Doc]](name: String,
                                                   model: Model,
                                                   store: Store[Doc, Model],
                                                   maxInsertBatch: Int = 1_000_000,
                                                   cacheQueries: Boolean = false) {
  def init(): Unit = {
    store.init(this)
  }

  def transaction[Return](f: Transaction[Doc] => Return): Return = {
    val transaction = store.createTransaction()
    try {
      f(transaction)
    } finally {
      store.releaseTransaction(transaction)
    }
  }

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = store.set(doc)

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(this)

  def dispose(): Unit = {
    store.dispose()
  }
}
