package lightdb.halodb

import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.Task

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class HaloDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     model: Model,
                                                                     directory: Path,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                                                                     maxFileSize: Int = 1024 * 1024 * 1024) extends Store[Doc, Model](name, model) {
  private val instance: HaloDB = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)
    opts.setMaxFileSize(maxFileSize)
    opts.setUseMemoryPool(true)
    opts.setMemoryPoolChunkSize(16 * 1024 * 1024)
    opts.setFlushDataSizeBytes(128 * 1024 * 1024)
    opts.setCompactionThresholdPerFile(0.9)
    opts.setCompactionJobRate(64 * 1024 * 1024)
    opts.setNumberOfRecords(100_000_000)
    opts.setCleanUpTombstonesDuringOpen(true)

    Files.createDirectories(directory.getParent)
    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val json = doc.json(model.rw)
    instance.put(id(doc).bytes, JsonFormatter.Compact(json).getBytes("UTF-8"))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = get(idField, id).map(_.nonEmpty)

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(instance.get(value.asInstanceOf[Id[Doc]].bytes)).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = {
    val json = bytes2Json(bytes)
    json.as[Doc](model.rw)
  }

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, "UTF-8")
    JsonParser(jsonString)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    instance.delete(value.asInstanceOf[Id[Doc]].bytes)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(instance.size().toInt)

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream
    .fromIterator(Task(instance.newIterator().asScala.map(_.getValue).map(bytes2Doc)))

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(instance.newIterator().asScala.map(_.getValue).map(bytes2Json)))

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    Task.error(new UnsupportedOperationException("HaloDBStore does not support searching"))

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("HaloDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    Task.error(new UnsupportedOperationException("HaloDBStore does not support aggregation"))

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    val size = instance.size().toInt
    if (size == 0) {
      0
    } else {
      instance.newIterator().asScala.foreach(r => instance.delete(r.getKey))
      size + truncate().sync()
    }
  }

  override def dispose(): Task[Unit] = Task {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new HaloDBStore[Doc, Model](name, model, db.directory.get.resolve(name), storeMode)
}