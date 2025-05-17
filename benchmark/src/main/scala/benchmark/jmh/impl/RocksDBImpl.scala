package benchmark.jmh.impl

import org.apache.commons.io.FileUtils
import org.rocksdb._
import rapid.Task

import java.io.File
import scala.util.Random

case class RocksDBImpl() extends BenchmarkImplementation {
  private var db: RocksDB = _
  private var path: File = _

  override def init: Task[Unit] = Task {
    path = new File("benchdb-rocks")
    FileUtils.deleteDirectory(path)
    path.mkdirs()

    RocksDB.loadLibrary()
    val options = new Options().setCreateIfMissing(true)
    db = RocksDB.open(options, path.getAbsolutePath)
  }

  override def insert(iterations: Int): Task[Unit] = Task {
    val writeOptions = new WriteOptions()
    (0 until iterations).foreach { _ =>
      val id = Random.alphanumeric.take(12).mkString
      val value = Random.alphanumeric.take(24).mkString
      db.put(writeOptions, id.getBytes("UTF-8"), value.getBytes("UTF-8"))
    }
    db.flush(new FlushOptions().setWaitForFlush(true))
  }

  override def count: Task[Int] = Task {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count = 0
    while (iter.isValid) {
      count += 1
      iter.next()
    }
    iter.close()
    count
  }

  override def read: Task[Int] = Task {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count = 0
    while (iter.isValid) {
      val key = new String(iter.key(), "UTF-8")
      val value = new String(iter.value(), "UTF-8")
      require(key.nonEmpty && value.nonEmpty)
      count += 1
      iter.next()
    }
    iter.close()
    count
  }

  override def traverse: Task[Unit] = Task {
    // RocksDB has no native graph traversal, simulate prefix scan on ASCII prefix 'r'
    val iter = db.newIterator()
    iter.seek("r".getBytes("UTF-8"))
    while (iter.isValid && new String(iter.key()).startsWith("r")) {
      iter.next()
    }
    iter.close()
  }

  override def dispose: Task[Unit] = Task {
    db.close()
    FileUtils.deleteDirectory(path)
  }
}