package lightdb.store.split

import fabric.Json
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import lightdb._
import lightdb.field.Field._
import rapid.Task

import scala.language.implicitConversions

case class SplitStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](override val name: String,
                                                                         model: Model,
                                                                         storage: Store[Doc, Model],
                                                                         searching: Store[Doc, Model],
                                                                         storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] =
    storage.prepareTransaction(transaction).and(searching.prepareTransaction(transaction)).unit

  private def ignoreSearchUpdates(implicit transaction: Transaction[Doc]): Boolean =
    transaction.get(SplitStore.NoSearchUpdates).contains(true)

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    storage.insert(doc).flatMap { doc =>
      if (!ignoreSearchUpdates) {
        searching.insert(doc)
      } else {
        Task.pure(doc)
      }
    }
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    storage.upsert(doc).flatMap { doc =>
      if (!ignoreSearchUpdates) {
        searching.upsert(doc)
      } else {
        Task.pure(doc)
      }
    }
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = storage.exists(id)

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] =
    storage.get(field, value)

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] =
    storage.delete(field, value).flatTap { b =>
      if (!ignoreSearchUpdates && b) {
        searching.delete(field, value)
      } else {
        Task.unit
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = storage.count

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = storage.stream

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = storage.jsonStream

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    searching.doSearch[V](query, conversion)

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    searching.aggregate(query)

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    searching.aggregateCount(query)

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] =
    storage.truncate().and(searching.truncate()).map(_._1)

  override def verify(): Task[Boolean] = transaction { implicit transaction =>
    storage.count.and(searching.count).flatMap {
      case (storageCount, searchCount) => if (storageCount != searchCount && model.fields.count(_.indexed) > 1) {
        scribe.warn(s"$name out of sync! Storage Count: $storageCount, Search Count: $searchCount. Re-Indexing...")
        reIndexInternal().map { _ =>
          scribe.info(s"$name re-indexed successfully!")
          true
        }
      } else {
        Task.pure(false)
      }
    }
  }

  override def reIndex(): Task[Boolean] = transaction { implicit transaction =>
    reIndexInternal().map(_ => true)
  }

  private def reIndexInternal()(implicit transaction: Transaction[Doc]): Task[Unit] = {
    // TODO: Process concurrently
    searching.truncate().flatMap { _ =>
      storage.stream.evalMap(searching.insert).drain
    }
  }

  override def dispose(): Task[Unit] = storage.dispose().and(searching.dispose()).unit
}

object SplitStore {
  val NoSearchUpdates: TransactionKey[Boolean] = TransactionKey[Boolean]("splitStoreNoSearchUpdates")
}