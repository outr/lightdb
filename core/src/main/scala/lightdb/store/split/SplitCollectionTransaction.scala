package lightdb.store.split

import fabric.Json
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Collection, Store}
import lightdb.transaction.{CollectionTransaction, RollbackSupport, Transaction}
import lightdb.{Query, SearchResults}
import rapid.Task

case class SplitCollectionTransaction[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
](store: SplitCollection[Doc, Model, Storage, Searching],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]) extends CollectionTransaction[Doc, Model] with RollbackSupport[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private[split] var _storage: store.storage.TX = _
  private[split] var _searching: store.searching.TX = _

  def storage: store.storage.TX = _storage

  def searching: store.searching.TX = _searching

  /**
   * Defines the mode for how updates apply to the search collection. Defaults to Immediate.
   */
  var searchUpdateHandler: SearchUpdateHandler[Doc, Model, Storage, Searching] = ImmediateUpdateHandler(this)

  def disableSearchUpdate(): Unit = searchUpdateHandler = DisabledUpdateHandler(this)

  override def jsonStream: rapid.Stream[Json] = storage.jsonStream

  override def _delete(id: Id[Doc]): Task[Boolean] =
    searchUpdateHandler.delete(id).next(storage.delete(id))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = if index == store.idField then {
    storage.get(value.asInstanceOf[Id[Doc]])
  } else {
    searching.get(_ => index -> value)
  }

  override protected def _insert(doc: Doc): Task[Doc] = storage.insert(doc).flatTap { _ =>
    searchUpdateHandler.insert(doc)
  }

  override protected def _upsert(doc: Doc): Task[Doc] = storage.upsert(doc).flatTap { _ =>
    searchUpdateHandler.upsert(doc)
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = storage.exists(id)

  override protected def _count: Task[Int] = storage.count

  override def estimatedCount: Task[Int] = storage.estimatedCount

  override protected def _commit: Task[Unit] = for
    _ <- storage.commit
    _ <- searchUpdateHandler.commit
  yield ()

  override protected def _rollback: Task[Unit] = for
    _ <- SearchUpdateHandler.rollbackIfSupported(storage)
    _ <- searchUpdateHandler.rollback
  yield ()

  override protected def _close: Task[Unit] = for
    _ <- store.storage.transaction.release(storage)
    _ <- searchUpdateHandler.close
    _ <- store.searching.transaction.release(searching)
  yield ()

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    searching.doSearch(query)

  override def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] =
    searching.streamScored(query.copy(transaction = searching))

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    searching.aggregate(query)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    searching.aggregateCount(query)

  override def distinct[F](query: Query[Doc, Model, _],
                           field: lightdb.field.Field[Doc, F],
                           pageSize: Int): rapid.Stream[F] =
    searching.distinct(query.copy(transaction = searching), field, pageSize)

  override def truncate: Task[Int] = storage.truncate.flatTap { _ =>
    searchUpdateHandler.truncate
  }
}
