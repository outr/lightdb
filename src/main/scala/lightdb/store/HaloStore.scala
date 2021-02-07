package lightdb.store

import com.oath.halodb.{HaloDB, HaloDBOptions}
import lightdb.Id

import scala.concurrent.{ExecutionContext, Future}

class HaloStore(directory: String = "lightdb/store",
                indexThreads: Int = 8) extends ObjectStore {
  private val halo = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)

    HaloDB.open(directory, opts)
  }

  override def get[T](id: Id[T])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = Future {
    Option(halo.get(id.bytes))
  }

  override def put[T](id: Id[T], value: Array[Byte])(implicit ec: ExecutionContext): Future[Array[Byte]] = Future {
    halo.put(id.bytes, value)
  }.map(_ => value)

  override def delete[T](id: Id[T])(implicit ec: ExecutionContext): Future[Unit] = Future {
    halo.delete(id.bytes)
  }

  override def dispose()(implicit ec: ExecutionContext): Unit = halo.close()
}