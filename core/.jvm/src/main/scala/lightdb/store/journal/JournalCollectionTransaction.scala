package lightdb.store.journal

import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.{CollectionTransaction, Transaction, WriteHandler}
import lightdb.{Query, SearchResults}
import rapid.Task

case class JournalCollectionTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: JournalCollection[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]
) extends CollectionTransaction[Doc, Model] with JournalTransactionSupport[Doc, Model] {
  override lazy val writeHandler: WriteHandler[Doc, Model] = writeHandlerFactory(this)

  override def journalStore: JournalBacked[Doc, Model] = store

  private def collectionReader: Task[CollectionTransaction[Doc, Model]] =
    reader.map(_.asInstanceOf[CollectionTransaction[Doc, Model]])

  override protected def getByIndex[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] =
    collectionReader.flatMap(_.get(_ => index -> value))

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    collectionReader.flatMap(r => r.doSearch(query.copy(transaction = r)))

  override def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] =
    rapid.Stream.force(collectionReader.map(r => r.streamScored(query.copy(transaction = r))))

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    rapid.Stream.force(collectionReader.map(r => r.aggregate(query.copy(query = query.query.copy(transaction = r)))))

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    collectionReader.flatMap(r => r.aggregateCount(query.copy(query = query.query.copy(transaction = r))))

  override def distinct[F](query: Query[Doc, Model, _], field: Field[Doc, F], pageSize: Int): rapid.Stream[F] =
    rapid.Stream.force(collectionReader.map(r => r.distinct(query.copy(transaction = r), field, pageSize)))
}
