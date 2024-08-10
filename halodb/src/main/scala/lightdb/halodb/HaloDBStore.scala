package lightdb.halodb

import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.{Field, Id, LightDB, Query, SearchResults, UniqueIndex}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class HaloDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](directory: Path,
                                                                     val storeMode: StoreMode,
                                                                     indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                                                                     maxFileSize: Int = 1024 * 1024 * 1024) extends Store[Doc, Model] {
  private lazy val instance: HaloDB = {
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

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
    instance
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = ()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val json = doc.json(collection.model.rw)
    instance.put(id(doc).bytes, JsonFormatter.Compact(json).getBytes("UTF-8"))
  }

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] = {
    if (field == idField) {
      Option(instance.get(value.asInstanceOf[Id[Doc]].bytes)).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = {
    val jsonString = new String(bytes, "UTF-8")
    val json = JsonParser(jsonString)
    json.as[Doc](collection.model.rw)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Boolean = {
    instance.delete(value.asInstanceOf[Id[Doc]].bytes)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Int = instance.size().toInt

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = instance.newIterator().asScala
    .map(_.getValue).map(bytes2Doc)

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] =
    throw new UnsupportedOperationException("HaloDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("HaloDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int =
    throw new UnsupportedOperationException("HaloDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val size = count
    instance.newIterator().asScala.foreach(r => instance.delete(r.getKey))
    size
  }

  override def dispose(): Unit = {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] =
    new HaloDBStore[Doc, Model](db.directory.get.resolve(name), storeMode)
}