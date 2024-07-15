package lightdb.mapdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.{Field, Id, LightDB, Query, SearchResults}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class MapDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](directory: Option[Path],
                                                                    val storeMode: StoreMode) extends Store[Doc, Model] {
  private lazy val db: DB = {
    val maker = directory.map { path =>
      Files.createDirectories(path.getParent)
      DBMaker.fileDB(path.toFile)
    }.getOrElse(DBMaker.memoryDirectDB())
    maker.make()
  }
  private lazy val map: HTreeMap[String, String] = db.hashMap("map", Serializer.STRING, Serializer.STRING).createOrOpen()

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
    map.verify()
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = ()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = map.put(doc._id.value, toString(doc))

  override def get[V](field: Field.Unique[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    if (field == idField) {
      Option(map.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: Field.Unique[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Boolean =
    map.remove(value.asInstanceOf[Id[Doc]].value) != null

  override def count(implicit transaction: Transaction[Doc]): Int = map.size()

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = map.values()
    .iterator()
    .asScala
    .map(fromString)

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] =
    throw new UnsupportedOperationException("MapDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("MapDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int =
    throw new UnsupportedOperationException("MapDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val size = count
    map.clear()
    size
  }

  override def size: Long = -1L

  override def dispose(): Unit = {
    db.commit()
    db.close()
  }
}

object MapDBStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] =
    new MapDBStore[Doc, Model](db.directory.map(_.resolve(name)), storeMode)
}