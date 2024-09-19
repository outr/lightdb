package lightdb.store

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb._
import lightdb.Field._

import java.io.File

abstract class Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  protected var collection: Collection[Doc, Model] = _

  protected def id(doc: Doc): Id[Doc] = doc.asInstanceOf[Document[_]]._id.asInstanceOf[Id[Doc]]
  protected lazy val idField: UniqueIndex[Doc, Id[Doc]] = collection.model._id

  def storeMode: StoreMode

  protected lazy val fields: List[Field[Doc, _]] = collection.model.fields match {
    case fields if storeMode == StoreMode.Indexes => fields.filter(_.isInstanceOf[Indexed[_, _]])
    case fields => fields
  }

  protected def toString(doc: Doc): String = JsonFormatter.Compact(doc.json(collection.model.rw))
  protected def fromString(string: String): Doc = JsonParser(string).as[Doc](collection.model.rw)

  lazy val hasSpatial: Boolean = fields.exists(_.isSpatial)

  def init(collection: Collection[Doc, Model]): Unit = {
    this.collection = collection
  }

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

  def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                 (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V]

  def aggregate(query: AggregateQuery[Doc, Model])
               (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int

  def truncate()(implicit transaction: Transaction[Doc]): Int

  def verify(): Boolean = false

  def reIndex(): Boolean = false

  def dispose(): Unit
}

object Store {
  def determineSize(file: File): Long = if (file.isDirectory) {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}