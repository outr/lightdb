package lightdb.mapdb

import cats.effect.IO
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.RW
import lightdb.Id
import lightdb.document.Document
import lightdb.store.{JsonStore, Store, StoreManager}
import lightdb.transaction.Transaction
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import scala.jdk.CollectionConverters._
import java.nio.file.Path

case class MapDBStore[D <: Document[D]](directory: Option[Path],
                                        chunkSize: Int = 1024)
                                       (implicit val rw: RW[D]) extends JsonStore[D] {
  override type Serialized = String

  private lazy val maker: DBMaker.Maker = directory match {
    case Some(path) =>
      path.toFile.getParentFile.mkdirs()
      DBMaker.fileDB(path.toFile)
    case None => DBMaker.memoryDB()
  }
  private lazy val db: DB = maker.make()
  private lazy val map: HTreeMap[String, String] = db.hashMap("map", Serializer.STRING, Serializer.STRING).createOrOpen()

  override protected def initialize(): IO[Unit] = IO.blocking {
    map
  }

  override protected def json2Serialized(json: Json): IO[String] = IO(JsonFormatter.Compact(json))

  override protected def serialized2Json(serialized: String): IO[Json] = IO(JsonParser(serialized))

  override protected def setSerialized(id: Id[D],
                                       serialized: String,
                                       transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    map.put(id.value, serialized)
    true
  }

  override protected def getSerialized(id: Id[D],
                                       transaction: Transaction[D]): IO[Option[String]] = IO.blocking {
    Option(map.getOrDefault(id.value, null))
  }

  override def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keySet().iterator().asScala, chunkSize)
    .map(key => Id[D](key))

  override protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, String] = fs2.Stream
    .fromBlockingIterator[IO](map.values().iterator().asScala, chunkSize)

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking(map.size())

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    map.remove(id.value) != null
  }

  override def truncate()(implicit transaction: Transaction[D]): IO[Int] = IO.blocking {
    val size = map.size()
    map.clear()
    size
  }

  override def dispose(): IO[Unit] = IO.blocking(db.close())
}

object MapDBStore extends StoreManager {
  override protected def create[D <: Document[D]](name: String)(implicit rw: RW[D]): IO[Store[D]] = IO {
    // TODO: Fix path resolution
    new MapDBStore[D](Some(Path.of("db", name)))
  }
}