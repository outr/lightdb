package lightdb.store

import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.{Field, Query, SearchResults, Transaction}

abstract class Store[Doc, Model <: DocModel[Doc]] {
  def init(collection: Collection[Doc, Model]): Unit

  def createTransaction(): Transaction[Doc]

  def releaseTransaction(transaction: Transaction[Doc]): Unit

  def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit

  def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc]

  def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean

  def count(implicit transaction: Transaction[Doc]): Int

  def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc]

  def doSearch[V](query: Query[Doc, Model], conversion: Conversion[V])
                 (implicit transaction: Transaction[Doc]): SearchResults[Doc, V]

  def truncate()(implicit transaction: Transaction[Doc]): Int

  def dispose(): Unit

  sealed trait Conversion[V]

  object Conversion {
    case class Value[F](field: Field[Doc, F]) extends Conversion[F]
    case object Doc extends Conversion[Doc]
    case class Json(fields: List[Field[Doc, _]]) extends Conversion[fabric.Json]
    case class Converted[T](f: Doc => T) extends Conversion[T]
  }
}
