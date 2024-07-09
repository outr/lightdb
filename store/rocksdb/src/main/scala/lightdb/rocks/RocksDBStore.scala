package lightdb.rocks

import fabric.rw._
import lightdb.Id
import org.rocksdb.{RocksDB, RocksIterator}

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer
import lightdb._
import lightdb.document.{Document, SetType}
import lightdb.store._
import lightdb.transaction.Transaction

case class RocksDBStore[D <: Document[D]](directory: Path, internalCounter: Boolean = false)
                                      (implicit val rw: RW[D]) extends ByteStore[D] {
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

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = createStream { i =>
    Option(i.key()).map(key => Id[D](key.string))
  }

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] = createStream { i =>
    Option(i.value()).map(bytes2D)
  }

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = Option(db.get(id.bytes)).map(bytes2D)

  override def contains(id: Id[D])(implicit transaction: Transaction[D]): Boolean = db.keyExists(id.bytes)

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = {
    val `type` = if (!internalCounter) {
      SetType.Unknown
    } else if (contains(id)) {
      SetType.Replace
    } else {
      SetType.Insert
    }
    db.put(id.bytes, d2Bytes(doc))
    Some(`type`)
  }

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean = {
    val exists = contains(id)
    db.delete(id.bytes)
    exists
  }

  override def count(implicit transaction: Transaction[D]): Int = idIterator.size

  override def commit()(implicit transaction: Transaction[D]): Unit = ()

  override def dispose(): Unit = {
    db.close()
  }
}

object RocksDBStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new RocksDBStore(db.directory.get.resolve(name))
}