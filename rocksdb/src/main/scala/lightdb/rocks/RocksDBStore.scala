package lightdb.rocks

import fabric.rw.RW
import lightdb.Id
import org.rocksdb.{RocksDB, RocksIterator}

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer
import lightdb._
import lightdb.store._

case class RocksDBStore[D](directory: Path)(implicit val rw: RW[D]) extends ByteStore[D] {
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

  override def idIterator: Iterator[Id[D]] = createStream { i =>
    Option(i.key()).map(key => Id[D](key.string))
  }

  override def iterator: Iterator[D] = createStream { i =>
    Option(i.value()).map(bytes2D)
  }

  override def get(id: Id[D]): Option[D] = Option(db.get(id.bytes)).map(bytes2D)

  override def put(id: Id[D], doc: D): Boolean = {
    db.put(id.bytes, d2Bytes(doc))
    true
  }

  override def delete(id: Id[D]): Unit = {
    db.delete(id.bytes)
  }

  override def count: Int = idIterator.size

  override def commit(): Unit = ()

  override def dispose(): Unit = {
    db.close()
  }
}

object RocksDBStore extends StoreManager {
  override protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new RocksDBStore(db.directory.resolve(name))
}