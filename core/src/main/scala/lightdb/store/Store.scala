package lightdb.store

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb._
import lightdb.error.DocNotFoundException
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.lock.LockManager
import lightdb.trigger.CollectionTriggers

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val name: String,
                                                                        model: Model) {
  protected def id(doc: Doc): Id[Doc] = doc.asInstanceOf[Document[_]]._id.asInstanceOf[Id[Doc]]
  lazy val idField: UniqueIndex[Doc, Id[Doc]] = model._id

  lazy val lock: LockManager[Id[Doc], Doc] = new LockManager

  object trigger extends CollectionTriggers[Doc]

  def storeMode: StoreMode[Doc, Model]

  protected lazy val fields: List[Field[Doc, _]] = if (storeMode.isIndexes) {
    model.fields.filter(_.isInstanceOf[Indexed[_, _]])
  } else {
    model.fields
  }

  protected def toString(doc: Doc): String = JsonFormatter.Compact(doc.json(model.rw))
  protected def fromString(string: String): Doc = JsonParser(string).as[Doc](model.rw)

  lazy val hasSpatial: Boolean = fields.exists(_.isSpatial)

  final def createTransaction(): Transaction[Doc] = {
    val t = new Transaction[Doc]
    prepareTransaction(t)
    t
  }

  def prepareTransaction(transaction: Transaction[Doc]): Unit

  def releaseTransaction(transaction: Transaction[Doc]): Unit = {
    transaction.commit()
  }

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean

  def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc]

  def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean

  def count(implicit transaction: Transaction[Doc]): Int

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc]

  def jsonIterator(implicit transaction: Transaction[Doc]): Iterator[Json] = iterator.map(_.json(model.rw))

  def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                 (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V]

  def aggregate(query: AggregateQuery[Doc, Model])
               (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int

  def truncate()(implicit transaction: Transaction[Doc]): Int

  def verify(): Boolean = false

  def reIndex(): Boolean = false

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Doc = get(model._id, id).getOrElse {
    throw DocNotFoundException(name, "_id", id)
  }

  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Option[Doc] => Option[Doc])
            (implicit transaction: Transaction[Doc]): Option[Doc] = this.lock(id, get(idField, id), establishLock) { existing =>
    f(existing) match {
      case Some(doc) =>
        upsert(doc)
        Some(doc)
      case None if deleteOnNone =>
        delete(idField, id)
        None
      case None => None
    }
  }

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
      val transaction = createTransaction()
      set.add(transaction)
      trigger.transactionStart(transaction)
      transaction
    }

    def release(transaction: Transaction[Doc]): Unit = {
      if (Collection.LogTransactions) scribe.info(s"Releasing Transaction for $name")
      trigger.transactionEnd(transaction)
      releaseTransaction(transaction)
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

  def dispose(): Unit
}

object Store {
  def determineSize(file: File): Long = if (file.isDirectory) {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}