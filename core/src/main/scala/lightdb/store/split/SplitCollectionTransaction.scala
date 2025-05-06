package lightdb.store.split

import fabric.Json
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Collection, Store}
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.{Query, SearchResults}
import rapid.Task

case class SplitCollectionTransaction[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model]
](store: SplitCollection[Doc, Model, Storage, Searching],
  parent: Option[Transaction[Doc, Model]]) extends CollectionTransaction[Doc, Model] {
  private[split] var _storage: store.storage.TX = _
  private[split] var _searching: store.searching.TX = _

  def storage: store.storage.TX = _storage

  def searching: store.searching.TX = _searching

  /**
   * Set this to false to ignore data changes in this transaction not applying changes to the searching transaction.
   *
   * This is useful for large modifications of data to avoid massive slowdowns, but leads to indexing getting out of
   * sync. It is recommended when using this to reIndex immediately after finalizing the transaction when this is set
   * to false.
   *
   * Defaults to true.
   */
  var applySearchUpdates: Boolean = true

  override def jsonStream: rapid.Stream[Json] = storage.jsonStream

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = if (index == store.idField) {
    storage.get(value.asInstanceOf[Id[Doc]])
  } else {
    searching.get(_ => index -> value)
  }

  override protected def _insert(doc: Doc): Task[Doc] = storage.insert(doc).flatTap { _ =>
    searching.insert(doc).when(applySearchUpdates)
  }

  override protected def _upsert(doc: Doc): Task[Doc] = storage.upsert(doc).flatTap { _ =>
    searching.upsert(doc).when(applySearchUpdates)
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = storage.exists(id)

  override protected def _count: Task[Int] = storage.count

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] =
    storage.delete(_ => index -> value).flatTap { _ =>
      searching.delete(_ => index -> value).when(applySearchUpdates)
    }

  override protected def _commit: Task[Unit] = for {
    _ <- storage.commit
    _ <- searching.commit.when(applySearchUpdates)
  } yield ()

  override protected def _rollback: Task[Unit] = for {
    _ <- storage.rollback
    _ <- searching.rollback.when(applySearchUpdates)
  } yield ()

  override protected def _close: Task[Unit] = for {
    _ <- store.storage.transaction.release(storage.asInstanceOf[store.storage.TX])
    _ <- store.searching.transaction.release(searching.asInstanceOf[store.searching.TX])
  } yield ()

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    searching.doSearch(query)

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    searching.aggregate(query)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    searching.aggregateCount(query)

  override def truncate: Task[Int] = storage.truncate.flatTap { _ =>
    searching.truncate.when(applySearchUpdates)
  }
}
