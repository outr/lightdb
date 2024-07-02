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

  def transaction[Return](f: Transaction[Doc] => Return): Return = {
    val transaction = store.createTransaction()
    try {
      f(transaction)
    } finally {
      store.releaseTransaction(transaction)
    }
  }

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = store.set(doc)

  def set(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Unit = docs.foreach(set)

  def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] =
    store.get(field, value)

  def apply[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Doc =
    get[V](field, value).getOrElse(throw DocNotFoundException(name, field.name, value))

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc], ev: Model =:= DocumentModel[_]): Option[Doc] = transaction.mayLock(id, lock) {
    val idField = ev(model)._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]]
    f(get(idField, id)) match {
      case Some(doc) =>
        set(doc)
        Some(doc)
      case None if deleteOnNone =>
        delete(idField, id)
        None
      case None => None
    }
  }

  def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean =
    store.delete(field, value)

  def delete[V](field: Field.Unique[Doc, V], values: Seq[V])(implicit transaction: Transaction[Doc]): Int =
    values.map(v => delete(field, v)).count(identity)

  def count(implicit transaction: Transaction[Doc]): Int = store.count

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(this)

  // TODO: Delete Query

  def truncate()(implicit transaction: Transaction[Doc]): Int = store.truncate()

  def dispose(): Unit = {
    store.dispose()
  }
}
