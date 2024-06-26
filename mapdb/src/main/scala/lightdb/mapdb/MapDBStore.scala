package lightdb.mapdb

import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.store.{ByteStore, Store, StoreManager}
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import scala.jdk.CollectionConverters._
import java.nio.file.Path

case class MapDBStore[D](directory: Option[Path], chunkSize: Int = 1024)(implicit val rw: RW[D]) extends ByteStore[D] {
  private val maker: DBMaker.Maker = directory match {
    case Some(path) =>
      path.toFile.getParentFile.mkdirs()
      DBMaker.fileDB(path.toFile)
    case None => DBMaker.memoryDB()
  }
  private val db: DB = maker.make()
  private val map: HTreeMap[String, Array[Byte]] = db.hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen()

  override def idIterator: Iterator[Id[D]] = map.keySet().iterator().asScala
    .map(key => Id[D](key))

  override def iterator: Iterator[D] = map.values().iterator().asScala.map(bytes2D)

  override def get(id: Id[D]): Option[D] = Option(map.getOrDefault(id.value, null)).map(bytes2D)

  override def put(id: Id[D], doc: D): Boolean = {
    map.put(id.value, d2Bytes(doc))
    true
  }

  override def delete(id: Id[D]): Unit = map.remove(id.value)

  override def count: Int = map.size()

  override def commit(): Unit = db.commit()

  override def truncate(): Unit = map.clear()

  override def dispose(): Unit = db.close()
}

object MapDBStore extends StoreManager {
  override protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new MapDBStore(db.directory.map(_.resolve(name)))
}