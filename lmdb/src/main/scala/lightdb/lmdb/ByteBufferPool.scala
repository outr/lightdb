package lightdb.lmdb

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

class ByteBufferPool(startingSize: Int) {
  private val queue = new ConcurrentLinkedQueue[ByteBuffer]

  def get(neededSize: Int): ByteBuffer = Option(queue.poll()) match {
    case Some(bb) if bb.capacity() >= neededSize => bb.clear()
    case _ => create(neededSize)
  }

  private def create(neededSize: Int): ByteBuffer = {
    var actual = startingSize
    while (actual < neededSize) {
      actual *= 2
    }
    ByteBuffer.allocateDirect(actual)
  }

  def release(bb: ByteBuffer): Unit = queue.offer(bb)
}
