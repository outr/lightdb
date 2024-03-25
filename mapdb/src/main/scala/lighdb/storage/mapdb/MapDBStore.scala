package lighdb.storage.mapdb

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.{Document, Id, LightDB}
import lightdb.store.{ObjectData, ObjectStore, ObjectStoreSupport}
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap
import org.mapdb.{DB, DBMaker, DataInput2, DataOutput2, HTreeMap, Serializer}

import scala.jdk.CollectionConverters._
import java.nio.file.Path

case class MapDBStore(directory: Option[Path]) extends ObjectStore {
  private val maker: DBMaker.Maker = directory match {
    case Some(path) =>
      path.toFile.getParentFile.mkdirs()
      DBMaker.fileDB(path.toFile)
    case None => DBMaker.memoryDB()
  }
  private val db: DB = maker.make()
  private val map: HTreeMap[String, Array[Byte]] = db.hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen()

  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO(Option(map.getOrDefault(id.value, null)))

  override def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]] = IO(map.put(id.value, value))

  override def delete[T](id: Id[T]): IO[Unit] = IO(map.remove(id.value))

  override def dispose(): IO[Unit] = IO(db.close())

  override def count(): IO[Long] = IO(map.size())

  override def all[T](chunkSize: Int): fs2.Stream[IO, ObjectData[T]] = fs2.Stream
    .fromBlockingIterator[IO](map.entrySet().iterator().asScala, chunkSize)
    .map { pair =>
      val id = Id[T](pair.getKey)
      val value = pair.getValue
      ObjectData(id, value)
    }

  override def commit(): IO[Unit] = IO(db.commit())

  override def truncate(): IO[Unit] = IO(map.clear())
}