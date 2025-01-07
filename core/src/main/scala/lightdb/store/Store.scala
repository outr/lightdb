package lightdb.store

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DocNotFoundException
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.lock.LockManager
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb.trigger.CollectionTriggers
import lightdb.util.Disposable
import rapid.{Forge, Task}
import scribe.{rapid => logger}

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val name: String,
                                                                        model: Model) extends Disposable {
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

  lazy val hasSpatial: Task[Boolean] = Task(fields.exists(_.isSpatial))

  def prepareTransaction(transaction: Transaction[Doc]): Task[Unit]

  def releaseTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.commit()
  }

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc]

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc]

  def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean]

  def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]]

  def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean]

  def count(implicit transaction: Transaction[Doc]): Task[Int]

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc]

  def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = stream.map(_.json(model.rw))

  def doSearch[V](query: Query[Doc, Model, V])
                 (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]]

  def aggregate(query: AggregateQuery[Doc, Model])
               (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]]

  def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int]

  def truncate()(implicit transaction: Transaction[Doc]): Task[Int]

  def verify(): Task[Boolean] = Task.pure(false)

  def reIndex(): Task[Boolean] = Task.pure(false)

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Doc] = get(model._id, id).map(_.getOrElse {
    throw DocNotFoundException(name, "_id", id)
  })

  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]])
            (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    lock(id, get(idField, id), establishLock) { existing =>
      f(existing).flatMap {
        case Some(doc) => upsert(doc).map(_ => Some(doc))
        case None if deleteOnNone => delete(idField, id).map(_ => None)
        case None => Task.pure(None)
      }
    }
  }

  object transaction {
    private val set = ConcurrentHashMap.newKeySet[Transaction[Doc]]

    def active: Int = set.size()

    def apply[Return](f: Transaction[Doc] => Task[Return]): Task[Return] = create().flatMap { transaction =>
      f(transaction).guarantee(release(transaction))
    }

    def create(): Task[Transaction[Doc]] = for {
      _ <- logger.info(s"Creating new Transaction for $name").when(Collection.LogTransactions)
      transaction = new Transaction[Doc]
      _ <- prepareTransaction(transaction)
      _ = set.add(transaction)
      _ <- trigger.transactionStart(transaction)
    } yield transaction

    def release(transaction: Transaction[Doc]): Task[Unit] = for {
      _ <- logger.info(s"Releasing Transaction for $name").when(Collection.LogTransactions)
      _ <- trigger.transactionEnd(transaction)
      _ <- releaseTransaction(transaction)
      _ <- transaction.close()
      _ = set.remove(transaction)
    } yield ()

    def releaseAll(): Task[Int] = Task {
      val list = set.iterator().asScala.toList
      list.foreach { transaction =>
        release(transaction)
      }
      list.size
    }
  }
}

object Store {
  def determineSize(file: File): Long = if (file.isDirectory) {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}