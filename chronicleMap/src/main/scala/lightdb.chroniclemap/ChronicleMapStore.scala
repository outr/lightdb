package lightdb.chroniclemap

import lightdb.aggregate.AggregateQuery
import lightdb.{Id, LightDB, Query, SearchResults}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import net.openhft.chronicle.map.ChronicleMap
import rapid.Task

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class ChronicleMapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           model: Model,
                                                                           directory: Option[Path],
                                                                           val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  private lazy val db: ChronicleMap[String, String] = {
    val b = ChronicleMap
      .of(classOf[String], classOf[String])
      .name(name)
      .entries(1_000_000)
      .averageKeySize(32)
      .averageValueSize(1024)
      .maxBloatFactor(2.0)
    directory match {
      case Some(d) =>
        Files.createDirectories(d.getParent)
        b.createPersistedTo(d.toFile)
      case None => b.create()
    }
  }

  override protected def initialize(): Task[Unit] = Task(db)

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    if (db.putIfAbsent(doc._id.value, toString(doc)) != null) {
      throw new RuntimeException(s"${doc._id.value} already exists")
    }
    doc
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    db.put(doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    db.containsKey(id.value)
  }

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(db.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    if (field == idField) {
      db.remove(value.asInstanceOf[Id[Doc]].value) != null
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(db.size())

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream.fromIterator(Task {
    db.values()
      .iterator()
      .asScala
      .map(fromString)
  })

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("ChronicleMapStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("ChronicleMapStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("ChronicleMapStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    db.clear()
    size
  }

  override protected def doDispose(): Task[Unit] = Task {
    db.close()
  }
}

object ChronicleMapStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new ChronicleMapStore[Doc, Model](name, model, db.directory.map(_.resolve(name)), storeMode)
}
