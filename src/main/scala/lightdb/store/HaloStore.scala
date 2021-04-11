package lightdb.store

import cats.effect.IO
import com.oath.halodb.{HaloDB, HaloDBOptions}
import lightdb.collection.Collection
import lightdb.{Document, Id, LightDB}

import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContext, Future}

case class HaloStore(directory: Path, indexThreads: Int = 8) extends ObjectStore {
  private val halo = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)

    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO {
    Option(halo.get(id.bytes))
  }

  override def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]] = IO {
    halo.put(id.bytes, value)
  }.map(_ => value)

  override def delete[T](id: Id[T]): IO[Unit] = IO {
    halo.delete(id.bytes)
  }

  override def count(): IO[Long] = IO {
    halo.size()
  }

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO(halo.close())
}