package testdb
import com.oath.halodb.{HaloDB, HaloDBOptions}

import scala.concurrent.{ExecutionContext, Future}

class HaloStore(directory: String = "lightdb",
                indexThreads: Int = 8)
               (override implicit val executionContext: ExecutionContext) extends ObjectStore {
  private val halo = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)

    HaloDB.open(directory, opts)
  }

  override def get[T](id: Id[T]): Future[Option[Array[Byte]]] = Future {
    Option(halo.get(id.bytes))
  }

  override def put[T](id: Id[T], value: Array[Byte]): Future[Array[Byte]] = Future {
    halo.put(id.bytes, value)
  }.map(_ => value)

  override def delete[T](id: Id[T]): Future[Unit] = Future {
    halo.delete(id.bytes)
  }

  override def dispose(): Unit = halo.close()
}