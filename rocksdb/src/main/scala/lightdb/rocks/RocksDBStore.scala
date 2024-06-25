package lightdb.rocks

import lightdb.Id
import org.rocksdb.{RocksDB, RocksIterator}

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer
import lightdb._
import lightdb.store._

case class RocksDBStore(directory: Path) extends Store {
  RocksDB.loadLibrary()

  private val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    RocksDB.open(directory.toAbsolutePath.toString)
  }

  private def createStream[T](f: RocksIterator => Option[T]): Iterator[T] = {
    val rocksIterator = db.newIterator()
    rocksIterator.seekToFirst()
    val iterator = new Iterator[Option[T]] {
      override def hasNext: Boolean = rocksIterator.isValid

      override def next(): Option[T] = try {
        f(rocksIterator)
      } finally {
        rocksIterator.next()
      }
    }
    iterator.flatten
  }

  override def keyStream[D]: Iterator[Id[D]] = createStream { i =>
    Option(i.key()).map(key => Id[D](key.string))
  }

  override def stream: Iterator[Array[Byte]] = createStream { i =>
    Option(i.value())
  }

  override def get[D](id: Id[D]): Option[Array[Byte]] = Option(db.get(id.bytes))

  override def put[D](id: Id[D], value: Array[Byte]): Boolean = {
    db.put(id.bytes, value)
    true
  }

  override def delete[D](id: Id[D]): Unit = {
    db.delete(id.bytes)
  }

  override def count: Int = keyStream.size

  override def commit(): Unit = ()

  override def dispose(): Unit = {
    db.close()
  }
}

object RocksDBStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new RocksDBStore(db.directory.resolve(name))
}