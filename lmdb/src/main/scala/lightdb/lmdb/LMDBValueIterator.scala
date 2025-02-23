package lightdb.lmdb

import org.lmdbjava.{Dbi, Txn}

import java.nio.ByteBuffer

class LMDBValueIterator(dbi: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) extends Iterator[ByteBuffer] {
  private val cursor = dbi.openCursor(txn)
  private var current: Option[ByteBuffer] = None
  private var open = true

  private def advanceCursor(): Boolean = {
    while (cursor.next()) {
      val bb = cursor.`val`()
      if (bb.remaining() > 0) {
        current = Some(bb)
        return true
      }
    }
    close()
    false
  }

  private def close(): Unit = {
    open = false
    cursor.close()
  }

  override def hasNext: Boolean = {
    if (open && current.isEmpty) advanceCursor()
    current.nonEmpty
  }

  override def next(): ByteBuffer = {
    if (!hasNext) throw new NoSuchElementException("No more values in iterator")
    val result = current.get
    current = None
    result
  }
}
