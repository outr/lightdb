package lightdb.mapdb

import fabric.rw.RW
import lightdb.document.{Document, SetType}
import lightdb.{Id, LightDB}
import lightdb.store.{ByteStore, Store, StoreManager}
import lightdb.transaction.Transaction
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import scala.jdk.CollectionConverters._
import java.nio.file.Path

case class MapDBStore[D <: Document[D]](directory: Option[Path], chunkSize: Int = 1024)(implicit val rw: RW[D]) extends ByteStore[D] {
  private val maker: DBMaker.Maker = directory match {
    case Some(path) =>
      path.toFile.getParentFile.mkdirs()
      DBMaker.fileDB(path.toFile)
    case None => DBMaker.memoryDB()
  }
  private val db: DB = maker.make()
  private val map: HTreeMap[String, Array[Byte]] = db.hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen()

  override def internalCounter: Boolean = true

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = map.keySet().iterator().asScala
    .map(key => Id[D](key))

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] = map.values().iterator().asScala.map(bytes2D)

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = Option(map.getOrDefault(id.value, null)).map(bytes2D)

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = {
    val `type` = if (map.put(id.value, d2Bytes(doc)) != null) {
      SetType.Replace
    } else {
      SetType.Insert
    }
    Some(`type`)
  }

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean = map.remove(id.value) != null

  override def count(implicit transaction: Transaction[D]): Int = map.size()

  override def commit()(implicit transaction: Transaction[D]): Unit = db.commit()

  override def truncate()(implicit transaction: Transaction[D]): Unit = map.clear()

  override def dispose(): Unit = db.close()
}

object MapDBStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new MapDBStore(db.directory.map(_.resolve(name)))
}