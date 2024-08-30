package lightdb.collection

import fabric.Json
import fabric.define.DefType
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.util.Initializable
import lightdb.{Field, Id, Query, Unique, UniqueIndex}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

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
        val missing = jc.rw.definition match {
          case DefType.Obj(map, _) => map.keys.filterNot { fieldName =>
            fieldNames.contains(fieldName)
          }.toList
          case _ => Nil
        }
        if (missing.nonEmpty) {
          throw ModelMissingFieldsException(name, missing)
        }
      case _ => // Can't do validation
    }

    // Verify the data is in-sync
    verify()
  }

  def verify(): Boolean = store.verify()

  def reIndex(): Boolean = store.reIndex()

  object transaction {
    private val set = ConcurrentHashMap.newKeySet[Transaction[Doc]]

    def active: Int = set.size()

    def apply[Return](f: Transaction[Doc] => Return): Return = {
      val transaction = create()
      try {
        f(transaction)
      } finally {
        release(transaction)
      }
    }

    def create(): Transaction[Doc] = {
      if (Collection.LogTransactions) scribe.info(s"Creating new Transaction for $name")
      val transaction = store.createTransaction()
      set.add(transaction)
      transaction
    }

    def release(transaction: Transaction[Doc]): Unit = {
      if (Collection.LogTransactions) scribe.info(s"Releasing Transaction for $name")
      store.releaseTransaction(transaction)
      transaction.close()
      set.remove(transaction)
    }

    def releaseAll(): Int = {
      val list = set.iterator().asScala.toList
      list.foreach { transaction =>
        release(transaction)
      }
      list.size
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

    def get[V](f: Model => (UniqueIndex[Doc, V], V)): Option[Doc] = transaction { implicit transaction =>
      collection.get(f)
    }

    def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Doc = transaction { implicit transaction =>
      collection(f)
    }

    def get(id: Id[Doc]): Option[Doc] = transaction { implicit transaction =>
      collection.get(id)
    }

    def apply(id: Id[Doc]): Doc = transaction { implicit transaction =>
      collection(id)
    }

    object json {
      def insert(iterator: Iterator[Json]): Int = transaction { implicit transaction =>
        iterator
          .map(_.as[Doc](model.rw))
          .map(doc => collection.insert(doc))
          .length
      }

      def iterator[Return](f: Iterator[Json] => Return): Return = transaction { implicit transaction =>
        val iterator = collection.iterator.map(doc => doc.json(model.rw))
        f(iterator)
      }
    }

    def list(): List[Doc] = transaction { implicit transaction =>
      collection.list()
    }

    def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
              (f: Option[Doc] => Option[Doc]): Option[Doc] = transaction { implicit transaction =>
      collection.modify(id, lock, deleteOnNone)(f)
    }

    def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Boolean = transaction { implicit transaction =>
      collection.delete(f)
    }

    def delete(id: Id[Doc]): Boolean = transaction { implicit transaction =>
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

  def get[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Option[Doc] = {
    val (field, value) = f(model)
    store.get(field, value)
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Doc =
    get[V](f).getOrElse {
      val (field, value) = f(model)
      throw DocNotFoundException(name, field.name, value)
    }

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): Option[Doc] = {
    store.get(model._id, id)
  }

  def getAll(ids: Seq[Id[Doc]])(implicit transaction: Transaction[Doc]): Iterator[Doc] = ids
    .iterator
    .flatMap(get)

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Doc =
    store.get(model._id, id).getOrElse {
      throw DocNotFoundException(name, "_id", id)
    }

  def list()(implicit transaction: Transaction[Doc]): List[Doc] = iterator.toList

  def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): Option[Doc] = transaction.mayLock(id, lock) {
    f(get(_ => model._id -> id)) match {
      case Some(doc) =>
        upsert(doc)
        Some(doc)
      case None if deleteOnNone =>
        delete(_ => model._id -> id)
        None
      case None => None
    }
  }

  def getOrCreate(id: Id[Doc], create: => Doc, lock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): Doc = modify(id, lock = lock) {
    case Some(doc) => Some(doc)
    case None => Some(create)
  }.get

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Boolean = {
    val (field, value) = f(model)
    store.delete(field, value)
  }

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Boolean = {
    store.delete(ev(model)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]], id)
  }

  def count(implicit transaction: Transaction[Doc]): Int = store.count

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(this)

  def truncate()(implicit transaction: Transaction[Doc]): Int = store.truncate()

  def dispose(): Unit = try {
    val transactions = transaction.releaseAll()
    if (transactions > 0) {
      scribe.warn(s"Released $transactions active transactions")
    }
  } finally {
    store.dispose()
  }
}

object Collection {
  var DefaultCacheQueries: Boolean = false
  var LogTransactions: Boolean = false
}