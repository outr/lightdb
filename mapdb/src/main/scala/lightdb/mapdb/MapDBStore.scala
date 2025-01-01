package lightdb.mapdb

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
                                                                    val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  private lazy val db: DB = {
    val maker = directory.map { path =>
      Files.createDirectories(path.getParent)
      DBMaker.fileDB(path.toFile)
    }.getOrElse(DBMaker.memoryDirectDB())
    maker.make()
  }
  private lazy val map: HTreeMap[String, String] = db.hashMap("map", Serializer.STRING, Serializer.STRING).createOrOpen()

  map.verify()

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    map.put(doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    map.containsKey(id.value)
  }

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(map.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] =
    Task(map.remove(value.asInstanceOf[Id[Doc]].value) != null)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(map.size())

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream.fromIterator(Task {
    map.values()
      .iterator()
      .asScala
      .map(fromString)
  })

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("MapDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("MapDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("MapDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    map.clear()
    size
  }

  override protected def doDispose(): Task[Unit] = Task {
    db.commit()
    db.close()
  }
}

object MapDBStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new MapDBStore[Doc, Model](name, model, db.directory.map(_.resolve(name)), storeMode)
}