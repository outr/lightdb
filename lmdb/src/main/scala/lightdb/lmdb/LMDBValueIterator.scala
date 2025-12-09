package lightdb.lmdb

import org.lmdbjava.{Dbi, GetOp, Txn}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class LMDBValueIterator(dbi: Dbi[ByteBuffer], txn: Txn[ByteBuffer], prefix: Option[String] = None) extends Iterator[ByteBuffer] {
  private val cursor = dbi.openCursor(txn)
  private var current: Option[ByteBuffer] = None
  private var open = true
  private val prefixBytes: Option[Array[Byte]] = prefix.map(_.getBytes(StandardCharsets.UTF_8))

  // Position cursor at first matching key (or first key if no prefix)
  prefixBytes match {
    case Some(pb) =>
      val keyBuffer = ByteBuffer.allocateDirect(pb.length)
      keyBuffer.put(pb).flip()
      val found = cursor.get(keyBuffer, GetOp.MDB_SET_RANGE)
      if (!found || !keyMatchesPrefix) {
        close()
      } else {
        val bb = cursor.`val`()
        if (bb != null && bb.remaining() > 0) {
          current = Some(bb)
        } else {
          advanceCursor()
        }
      }
    case None =>
      advanceCursor()
  }

  private def close(): Unit = {
    open = false
    cursor.close()
  }

  private def keyMatchesPrefix: Boolean = prefixBytes match {
    case None => true
    case Some(pb) =>
      val k = cursor.key()
      if (k == null || k.remaining() < pb.length) {
        false
      } else {
        val arr = new Array[Byte](pb.length)
        k.get(arr)
        k.position(k.position() - pb.length) // reset position after read
        java.util.Arrays.equals(arr, pb)
      }
  }

  private def advanceCursor(): Boolean = {
    while (open && cursor.next()) {
      if (keyMatchesPrefix) {
        val bb = cursor.`val`()
        if (bb != null && bb.remaining() > 0) {
          current = Some(bb)
          return true
        }
      } else if (prefixBytes.nonEmpty) {
        // keys are sorted; once prefix no longer matches, stop
        close()
      }
    }
    close()
    false
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
