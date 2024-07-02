package lightdb.collection

import lightdb.doc.{DocModel, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.store.Store
import lightdb.util.Initializable
import lightdb.{Field, Id, Query, Transaction}

case class Collection[Doc, Model <: DocModel[Doc]](name: String,
                                                   model: Model,
                                                   store: Store[Doc, Model],
                                                   maxInsertBatch: Int = 1_000_000,
                                                   cacheQueries: Boolean = false) extends Initializable {
  override protected def initialize(): Unit = {
    store.init(this)
  }

  object transaction {
    def apply[Return](f: Transaction[Doc] => Return): Return = {
      val transaction = create()
      try {
        f(transaction)
      } finally {
        release(transaction)
      }
    }

    def create(): Transaction[Doc] = store.createTransaction()

    def release(transaction: Transaction[Doc]): Unit = store.releaseTransaction(transaction)
  }

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): Doc = {
    store.set(doc)
    doc
  }

  def set(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Seq[Doc] = docs.map(set)

  def get[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): Option[Doc] = {
    val (field, value) = f(model)
    store.get(field, value)
  }

  def apply[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): Doc =
    get[V](f).getOrElse {
      val (field, value) = f(model)
      throw DocNotFoundException(name, field.name, value)
    }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): Option[Doc] = transaction.mayLock(id, lock) {
    val idField = model.asInstanceOf[DocumentModel[_]]._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]]
    f(get(_ => idField -> id)) match {
      case Some(doc) =>
        set(doc)
        Some(doc)
      case None if deleteOnNone =>
        delete(_ => idField -> id)
        None
      case None => None
    }
  }

  def delete[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): Boolean = {
    val (field, value) = f(model)
    store.delete(field, value)
  }

  def count(implicit transaction: Transaction[Doc]): Int = store.count

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(this)

  // TODO: Delete Query

  def truncate()(implicit transaction: Transaction[Doc]): Int = store.truncate()

  def dispose(): Unit = {
    store.dispose()
  }
}
