package lightdb.mapdb

import cats.effect.IO
import lightdb.{Id, Store}
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

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keySet().iterator().asScala, chunkSize)
    .map(key => Id[D](key))

  override def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = fs2.Stream
    .fromBlockingIterator[IO](map.entrySet().iterator().asScala, chunkSize)
    .map { pair =>
      Id[D](pair.getKey) -> pair.getValue
    }

  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO(Option(map.getOrDefault(id.value, null)))

  override def put[T](id: Id[T], value: Array[Byte]): IO[Boolean] = IO {
    map.put(id.value, value)
    true
  }

  override def delete[T](id: Id[T]): IO[Unit] = IO(map.remove(id.value))

  override def size: IO[Int] = IO(map.size())

  override def commit(): IO[Unit] = IO(db.commit())

  override def truncate(): IO[Unit] = IO(map.clear())

  override def dispose(): IO[Unit] = IO(db.close())
}