package lightdb.collection

import fabric.Json
import fabric.define.DefType
import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
import lightdb.field.Field._
import lightdb.lock.LockManager
import lightdb.store.split.SplitStore
import lightdb.store.{Conversion, Store}
import lightdb.transaction.Transaction
import lightdb.util.{Disposable, Initializable}
import rapid._
import scribe.{rapid => logger}

case class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         model: Model,
                                                                         store: Store[Doc, Model],
                                                                         db: LightDB) extends Initializable with Disposable { collection =>
  def lock: LockManager[Id[Doc], Doc] = store.lock

  def trigger: store.trigger.type = store.trigger

  override protected def initialize(): Task[Unit] = store.init.next {
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
    model.init(this).flatMap { _ =>
      // Verify the data is in-sync
      verify()
    }
  }.unit

  def verify(): Task[Boolean] = store.verify()

  def reIndex(): Task[Boolean] = store.reIndex()

  def reIndex(doc: Doc): Task[Boolean] = store.reIndex(doc)

  def transaction: store.transaction.type = store.transaction

  /**
   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
   */
  object t {
    def insert(doc: Doc): Task[Doc] = transaction { implicit transaction =>
      collection.insert(doc)
    }

    def upsert(doc: Doc): Task[Doc] = transaction { implicit transaction =>
      collection.upsert(doc)
    }

    def insert(docs: Seq[Doc]): Task[Seq[Doc]] = transaction { implicit transaction =>
      collection.insert(docs)
    }

    def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = transaction { implicit transaction =>
      collection.upsert(docs)
    }

    def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = transaction { implicit transaction =>
      collection.get(f)
    }

    def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = transaction { implicit transaction =>
      collection(f)
    }

    def get(id: Id[Doc]): Task[Option[Doc]] = transaction { implicit transaction =>
      collection.get(id)
    }

    def getAll(ids: Seq[Id[Doc]]): Task[List[Doc]] = transaction { implicit transaction =>
      collection.getAll(ids).toList
    }

    def apply(id: Id[Doc]): Task[Doc] = transaction { implicit transaction =>
      collection(id)
    }

    object json {
      def insert(stream: rapid.Stream[Json],
                 disableSearchUpdates: Boolean): Task[Int] = transaction { implicit transaction =>
        if (disableSearchUpdates) {
          transaction.put(SplitStore.NoSearchUpdates, true)
        }
        stream
          .map(_.as[Doc](model.rw))
          .evalMap(collection.insert)
          .count
      }

      def stream[Return](f: rapid.Stream[Json] => Task[Return]): Task[Return] = transaction { implicit transaction =>
        f(collection.store.jsonStream)
      }
    }

    def list(): Task[List[Doc]] = transaction { implicit transaction =>
      collection.list()
    }

    def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
              (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = transaction { implicit transaction =>
      collection.modify(id, lock, deleteOnNone)(f)
    }

    def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = transaction { implicit transaction =>
      collection.delete(f)
    }

    def delete(id: Id[Doc]): Task[Boolean] = transaction { implicit transaction =>
      collection.delete(id)
    }

    def count: Task[Int] = transaction { implicit transaction =>
      collection.count
    }

    def truncate(): Task[Int] = transaction { implicit transaction =>
      collection.truncate()
    }
  }

  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = trigger.insert(doc).flatMap { _ =>
    store.insert(doc)
  }

  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = trigger.upsert(doc).flatMap { _ =>
    store.upsert(doc)
  }

  def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = for {
    _ <- docs.map(trigger.insert).tasks
    _ <- store.insert(docs)
  } yield docs

  def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = for {
    _ <- docs.map(trigger.upsert).tasks
    _ <- store.upsert(docs)
  } yield docs

  def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = store.exists(id)

  def get[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    val (field, value) = f(model)
    store.get(field, value)
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Doc] =
    get[V](f).map {
      case Some(doc) => doc
      case None =>
        val (field, value) = f(model)
        throw DocNotFoundException(name, field.name, value)
    }

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = store.get(model._id, id)

  def getAll(ids: Seq[Id[Doc]])(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream
    .emits(ids)
    .evalMap(apply)

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Doc] = store(id)

  def list()(implicit transaction: Transaction[Doc]): Task[List[Doc]] = stream.toList

  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]])
            (implicit transaction: Transaction[Doc]): Task[Option[Doc]] =
    store.modify(id, establishLock, deleteOnNone)(f)

  def getOrCreate(id: Id[Doc], create: => Doc, establishLock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): Task[Doc] = modify(id, establishLock = establishLock) {
    case Some(doc) => Task.pure(Some(doc))
    case None => Task.pure(Some(create))
  }.map(_.get)

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Boolean] = {
    val (field, value) = f(model)
    trigger.delete(field, value).flatMap(_ => store.delete(field, value))
  }

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Task[Boolean] = {
    trigger.delete(ev(model)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]], id).flatMap { _ =>
      store.delete(ev(model)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]], id)
    }
  }

  def count(implicit transaction: Transaction[Doc]): Task[Int] = store.count

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = store.stream

  lazy val query: Query[Doc, Model, Doc] = Query(model, store, Conversion.Doc())

  def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = {
    trigger.truncate().flatMap(_ => store.truncate())
  }

  override protected def doDispose(): Task[Unit] = transaction.releaseAll().flatMap { transactions =>
    logger.warn(s"Released $transactions active transactions").when(transactions > 0)
  }.guarantee(trigger.dispose().flatMap(_ => store.dispose))
}

object Collection {
  var CacheQueries: Boolean = false
  var MaxInsertBatch: Int = 1_000_000
  var LogTransactions: Boolean = false
}