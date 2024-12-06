package lightdb.collection

import fabric.Json
import fabric.define.DefType
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.trigger.CollectionTriggers
import lightdb.util.Initializable
import lightdb._
import lightdb.field.Field._
import lightdb.lock.LockManager

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

case class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         model: Model,
                                                                         store: Store[Doc, Model]) extends Initializable { collection =>
  def lock: LockManager[Id[Doc], Doc] = store.lock

  def trigger: store.trigger.type = store.trigger

  override protected def initialize(): Unit = {
    model match {
      case jc: JsonConversion[_] =>
        val fieldNames = model.fields.map(_.name).toSet
        val missing = jc.rw.definition match {
          case DefType.Obj(map, _) => map.keys.filterNot { fieldName =>
            fieldNames.contains(fieldName)
          }.toList
          case DefType.Poly(values, _) =>
            values.values.flatMap(_.asInstanceOf[DefType.Obj].map.keys).filterNot { fieldName =>
              fieldNames.contains(fieldName)
            }.toList.distinct
          case _ => Nil
        }
        if (missing.nonEmpty) {
          throw ModelMissingFieldsException(name, missing)
        }
      case _ => // Can't do validation
    }

    // Give the Model a chance to initialize
    model.init(this)

    // Verify the data is in-sync
    verify()
  }

  def verify(): Boolean = store.verify()

  def reIndex(): Boolean = store.reIndex()

  def transaction: store.transaction.type = store.transaction

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

    def getAll(ids: Seq[Id[Doc]]): Iterator[Doc] = transaction { implicit transaction =>
      collection.getAll(ids)
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
        val iterator = collection.store.jsonIterator
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
    trigger.insert(doc)
    store.insert(doc)
    doc
  }

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Doc = {
    trigger.upsert(doc)
    store.upsert(doc)
    doc
  }

  def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Seq[Doc] = docs.map(insert)

  def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Seq[Doc] = docs.map(upsert)

  def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean = store.exists(id)

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

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Doc = store(id)

  def list()(implicit transaction: Transaction[Doc]): List[Doc] = iterator.toList

  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): Option[Doc] = store.modify(id, establishLock, deleteOnNone)(f)

  def getOrCreate(id: Id[Doc], create: => Doc, establishLock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): Doc = modify(id, establishLock = establishLock) {
    case Some(doc) => Some(doc)
    case None => Some(create)
  }.get

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Boolean = {
    val (field, value) = f(model)
    trigger.delete(field, value)
    store.delete(field, value)
  }

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Boolean = {
    trigger.delete(ev(model)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]], id)
    store.delete(ev(model)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]], id)
  }

  def count(implicit transaction: Transaction[Doc]): Int = store.count

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = store.iterator

  lazy val query: Query[Doc, Model] = Query(model, store)

  def truncate()(implicit transaction: Transaction[Doc]): Int = {
    trigger.truncate()
    store.truncate()
  }

  def dispose(): Unit = try {
    val transactions = transaction.releaseAll()
    if (transactions > 0) {
      scribe.warn(s"Released $transactions active transactions")
    }
  } finally {
    trigger.dispose()
    store.dispose()
  }
}

object Collection {
  var CacheQueries: Boolean = false
  var MaxInsertBatch: Int = 1_000_000
  var LogTransactions: Boolean = false
}