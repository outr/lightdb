package lightdb.mapdb

import fabric.Json
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}
import rapid.Task

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class MapDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                    model: Model,
                                                                    directory: Option[Path],
                                                                    val storeMode: StoreMode[Doc, Model],
                                                                    lightDB: LightDB,
                                                                    storeManager: StoreManager) extends Store[Doc, Model](name, model, lightDB, storeManager) {
  private lazy val db: DB = {
    val maker = directory.map { path =>
      Files.createDirectories(path.getParent)
      DBMaker.fileDB(path.toFile)
    }.getOrElse(DBMaker.memoryDirectDB())
    maker.make()
  }
  private lazy val map: HTreeMap[String, String] = db.hashMap("map", Serializer.STRING, Serializer.STRING).createOrOpen()

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(map.verify()))

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    map.put(doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    map.containsKey(id.value)
  }

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(map.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(map.remove(value.asInstanceOf[Id[Doc]].value) != null)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(map.size())

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    map.values()
      .iterator()
      .asScala
      .map(toJson)
  })

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    map.clear()
    size
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    db.commit()
    db.close()
  })
}

object MapDBStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = MapDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new MapDBStore[Doc, Model](name, model, db.directory.map(_.resolve(name)), storeMode, db, this)
}