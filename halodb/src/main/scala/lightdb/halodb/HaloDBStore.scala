package lightdb.halodb

import fabric._
import fabric.rw.{Asable, Convertible, RW}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid._
import scribe.{Level, Logger}

import java.nio.file.Path
import scala.language.implicitConversions

class HaloDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     instance: HaloDBInstance,
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  private implicit def rw: RW[Doc] = model.rw

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val json = doc.json(model.rw)
    instance.put(doc._id, json).map(_ => doc)
  }.flatten

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = _get(idField, id).map(_.nonEmpty)

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    if (field == idField) {
      instance.get(value.asInstanceOf[Id[Doc]]).map(_.map(_.as[Doc]))
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] =
    instance.delete(value.asInstanceOf[Id[Doc]]).map(_ => true)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = instance.count

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = instance.stream

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = instance.truncate()

  override protected def doDispose(): Task[Unit] = super.doDispose().next(instance.dispose())
}

object HaloDBStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = HaloDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): HaloDBStore[Doc, Model] = {
    Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()
    val instance = new DirectHaloDBInstance(path.get)
    new HaloDBStore[Doc, Model](name, path, model, storeMode, instance, db, this)
  }
}
