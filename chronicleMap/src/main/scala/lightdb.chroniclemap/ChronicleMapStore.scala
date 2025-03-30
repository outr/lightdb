package lightdb.chroniclemap

import fabric.Json
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.{Id, LightDB, Query, SearchResults}
import net.openhft.chronicle.map.ChronicleMap
import rapid.Task

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class ChronicleMapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           model: Model,
                                                                           directory: Option[Path],
                                                                           val storeMode: StoreMode[Doc, Model],
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager) extends Store[Doc, Model](name, model, lightDB, storeManager) {
  sys.props("net.openhft.chronicle.hash.impl.util.jna.PosixFallocate.fallocate") = "false"

  private lazy val db: ChronicleMap[String, String] = {
    val b = ChronicleMap
      .of(classOf[String], classOf[String])
      .name(name)
      .entries(1_000_000)
      .averageKeySize(32)
      .averageValueSize(5 * 1024)
      .maxBloatFactor(2.0)
      .sparseFile(true)
    directory match {
      case Some(d) =>
        Files.createDirectories(d.getParent)
        b.createPersistedTo(d.toFile)
      case None => b.create()
    }
  }

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(db))

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    if (db.putIfAbsent(doc._id.value, toString(doc)) != null) {
      throw new RuntimeException(s"${doc._id.value} already exists")
    }
    doc
  }

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    db.put(doc._id.value, toString(doc))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    db.containsKey(id.value)
  }

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(db.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    if (field == idField) {
      db.remove(value.asInstanceOf[Id[Doc]].value) != null
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(db.size())

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    db.values()
      .iterator()
      .asScala
      .map(toJson)
  })

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.map { size =>
    db.clear()
    size
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    db.close()
  })
}

object ChronicleMapStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = ChronicleMapStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new ChronicleMapStore[Doc, Model](name, model, db.directory.map(_.resolve(name)), storeMode, db, this)
}
