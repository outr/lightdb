package lightdb.lucene

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.{Field, LightDB, Query, SearchResults}
import lightdb.doc.DocModel
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction

import java.nio.file.Path

class LuceneStore[Doc, Model <: DocModel[Doc]](directory: Option[Path], val storeMode: StoreMode) extends Store[Doc, Model] {
  private lazy val index = directory match {
    case Some(dir) => FSIndex(dir)
    case None => new MemoryIndex
  }

  override def init(collection: Collection[Doc, Model]): Unit = ???

  override def createTransaction(): Transaction[Doc] = ???

  override def releaseTransaction(transaction: Transaction[Doc]): Unit = ???

  override def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = ???

  override def get[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] = ???

  override def delete[V](field: Field.Unique[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = ???

  override def count(implicit transaction: Transaction[Doc]): Int = ???

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = ???

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])(implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = ???

  override def aggregate(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] = ???

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int = ???

  override def truncate()(implicit transaction: Transaction[Doc]): Int = ???

  override def dispose(): Unit = ???
}

object LuceneStore extends StoreManager {
  override def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String, storeMode: StoreMode): Store[Doc, Model] =
    new LuceneStore[Doc, Model](db.directory.map(_.resolve(s"$name.lucene")), storeMode)
}