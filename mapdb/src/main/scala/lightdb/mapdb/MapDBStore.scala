package lightdb.mapdb

import lightdb.{Id, LightDB}
import lightdb.store.{Store, StoreManager}
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import scala.jdk.CollectionConverters._
import java.nio.file.Path

case class MapDBStore(directory: Option[Path], chunkSize: Int = 1024) extends Store {
  private val maker: DBMaker.Maker = directory match {
    case Some(path) =>
      path.toFile.getParentFile.mkdirs()
      DBMaker.fileDB(path.toFile)
    case None => DBMaker.memoryDB()
  }
  private val db: DB = maker.make()
  private val map: HTreeMap[String, Array[Byte]] = db.hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen()

  override def keyStream[D]: Iterator[Id[D]] = map.keySet().iterator().asScala
    .map(key => Id[D](key))

  override def stream: Iterator[Array[Byte]] = map.values().iterator().asScala

  override def get[T](id: Id[T]): Option[Array[Byte]] = Option(map.getOrDefault(id.value, null))

  override def put[T](id: Id[T], value: Array[Byte]): Boolean = {
    map.put(id.value, value)
    true
  }

  override def delete[T](id: Id[T]): Unit = map.remove(id.value)

  override def count: Int = map.size()

  override def commit(): Unit = db.commit()

  override def truncate(): Unit = map.clear()

  override def dispose(): Unit = db.close()
}

object MapDBStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new MapDBStore(Some(db.directory.resolve(name)))
}