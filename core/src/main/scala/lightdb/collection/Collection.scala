package lightdb.collection

import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.util.Initializable
import lightdb.{Field, Id, Query}

case class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         model: Model,
                                                                         loadStore: () => Store[Doc, Model],
                                                                         maxInsertBatch: Int = 1_000_000,
                                                                         cacheQueries: Boolean = Collection.DefaultCacheQueries) extends Initializable { collection =>
  lazy val store: Store[Doc, Model] = loadStore()

  override protected def initialize(): Unit = {
    store.init(this)

    model match {
      case jc: JsonConversion[_] =>
        val fieldNames = model.fields.map(_.name).toSet
        val missing = jc.rw.definition.asInstanceOf[DefType.Obj].map.keys.filterNot { fieldName =>
          fieldNames.contains(fieldName)
        }.toList
        if (missing.nonEmpty) {
          throw ModelMissingFieldsException(name, missing)
        }
      case _ => // Can't do validation
    }
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

    def release(transaction: Transaction[Doc]): Unit = {
      store.releaseTransaction(transaction)
      transaction.close()
    }
  }

  /**
   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
   */
  object t {
    def insert(doc: Doc): Doc = transaction { implicit transaction =>
      collection.insert(doc)
    }

    def upsert(doc: Doc): Doc = transaction { implicit transaction =>
      collection.upsert(doc)
    }

    def insert(docs: Seq[Doc]): Seq[Doc] = transaction { implicit transaction =>
      collection.insert(docs)
    }

    def upsert(docs: Seq[Doc]): Seq[Doc] = transaction { implicit transaction =>
      collection.upsert(docs)
    }

    def get[V](f: Model => (Field.Unique[Doc, V], V)): Option[Doc] = transaction { implicit transaction =>
      collection.get(f)
    }

    def apply[V](f: Model => (Field.Unique[Doc, V], V)): Doc = transaction { implicit transaction =>
      collection(f)
    }

    def get(id: Id[Doc]): Option[Doc] = transaction { implicit transaction =>
      collection.get(id)
    }

    def apply(id: Id[Doc]): Doc = transaction { implicit transaction =>
      collection(id)
    }

    def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
              (f: Option[Doc] => Option[Doc]): Option[Doc] = transaction { implicit transaction =>
      collection.modify(id, lock, deleteOnNone)(f)
    }

    def delete[V](f: Model => (Field.Unique[Doc, V], V)): Boolean = transaction { implicit transaction =>
      collection.delete(f)
    }

    def delete(id: Id[Doc])(implicit ev: Model <:< DocumentModel[_]): Boolean = transaction { implicit transaction =>
      collection.delete(id)
    }

    def count: Int = transaction { implicit transaction =>
      collection.count
    }

    def truncate(): Int = transaction { implicit transaction =>
      collection.truncate()
    }
  }

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Doc = {
    store.insert(doc)
    doc
  }

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Doc = {
    store.upsert(doc)
    doc
  }

  def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Seq[Doc] = docs.map(insert)

  def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Seq[Doc] = docs.map(upsert)

  def get[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): Option[Doc] = {
    val (field, value) = f(model)
    store.get(field, value)
  }

  def apply[V](f: Model => (Field.Unique[Doc, V], V))(implicit transaction: Transaction[Doc]): Doc =
    get[V](f).getOrElse {
      val (field, value) = f(model)
      throw DocNotFoundException(name, field.name, value)
    }

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): Option[Doc] = {
    store.get(model._id, id)
  }

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Doc =
    store.get(model._id, id).getOrElse {
      throw DocNotFoundException(name, "_id", id)
    }

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): Option[Doc] = transaction.mayLock(id, lock) {
    val idField = model.asInstanceOf[DocumentModel[_]]._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]]
    f(get(_ => idField -> id)) match {
      case Some(doc) =>
        upsert(doc)
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

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Boolean = {
    store.delete(ev(model)._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]], id)
  }

  def count(implicit transaction: Transaction[Doc]): Int = store.count

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(this)

  def truncate()(implicit transaction: Transaction[Doc]): Int = store.truncate()

  def dispose(): Unit = {
    store.dispose()
  }
}

object Collection {
  var DefaultCacheQueries: Boolean = false
}