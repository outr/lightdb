package lightdb.lmdb

import org.lmdbjava._
import rapid.Task

import java.nio.ByteBuffer

case class LMDBInstance(env: Env[ByteBuffer], val dbi: Dbi[ByteBuffer]) {
  private val keyPool = new ByteBufferPool(512)
  private val valuePool = new ByteBufferPool(512)

  def get(txn: Txn[ByteBuffer], key: Array[Byte]): Option[Array[Byte]] = {
    val keyBuf = keyPool.get(key.length)
    keyBuf.clear()
    keyBuf.put(key).flip()
    val result = Option(dbi.get(txn, keyBuf)).map { bb =>
      val arr = new Array[Byte](bb.remaining())
      bb.get(arr)
      arr
    }
    keyPool.release(keyBuf)
    result
  }

  def put(txn: Txn[ByteBuffer], key: Array[Byte], value: Array[Byte], overwrite: Boolean): Unit = {
    val keyBuf = keyPool.get(key.length)
    val valBuf = valuePool.get(value.length)
    keyBuf.clear(); keyBuf.put(key).flip()
    valBuf.clear(); valBuf.put(value).flip()
    if (overwrite)
      dbi.put(txn, keyBuf, valBuf)
    else
      dbi.put(txn, keyBuf, valBuf, PutFlags.MDB_NOOVERWRITE)
    keyPool.release(keyBuf)
    valuePool.release(valBuf)
  }

  def delete(txn: Txn[ByteBuffer], key: Array[Byte]): Boolean = {
    val keyBuf = keyPool.get(key.length)
    keyBuf.clear(); keyBuf.put(key).flip()
    val deleted = dbi.delete(txn, keyBuf)
    keyPool.release(keyBuf)
    deleted
  }

  def iterator(txn: Txn[ByteBuffer]): Iterator[(Array[Byte], Array[Byte])] = new Iterator[(Array[Byte], Array[Byte])] {
    private val cursor = dbi.openCursor(txn)
    private var hasNextChecked = false
    private var hasMore = cursor.first()

    override def hasNext: Boolean = {
      if (!hasNextChecked) {
        hasNextChecked = true
        hasMore
      } else hasMore
    }

    override def next(): (Array[Byte], Array[Byte]) = {
      if (!hasNext) throw new NoSuchElementException
      val key = {
        val k = cursor.key()
        val bytes = new Array[Byte](k.remaining())
        k.get(bytes)
        bytes
      }
      val value = {
        val v = cursor.`val`()
        val bytes = new Array[Byte](v.remaining())
        v.get(bytes)
        bytes
      }
      hasMore = cursor.next()
      hasNextChecked = false
      key -> value
    }
  }

  def prefixScan(txn: Txn[ByteBuffer], prefix: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = new Iterator[(Array[Byte], Array[Byte])] {
    private val cursor = dbi.openCursor(txn)
    private val prefixBuf = keyPool.get(prefix.length)
    prefixBuf.clear(); prefixBuf.put(prefix).flip()

    private var current: Option[(Array[Byte], Array[Byte])] = {
      if (cursor.get(prefixBuf, GetOp.MDB_SET_RANGE)) {
        val k = cursor.key()
        if (startsWith(k, prefix)) Some(readKV(cursor))
        else None
      } else None
    }

    override def hasNext: Boolean = current.isDefined

    override def next(): (Array[Byte], Array[Byte]) = {
      val result = current.getOrElse(throw new NoSuchElementException)
      if (cursor.next()) {
        val k = cursor.key()
        if (startsWith(k, prefix)) {
          current = Some(readKV(cursor))
        } else {
          current = None
        }
      } else {
        current = None
      }
      result
    }

    private def startsWith(buffer: ByteBuffer, prefix: Array[Byte]): Boolean = {
      if (buffer.remaining() < prefix.length) return false
      val origPos = buffer.position()
      for (i <- prefix.indices) {
        if (buffer.get() != prefix(i)) {
          buffer.position(origPos)
          return false
        }
      }
      buffer.position(origPos)
      true
    }

    private def readKV(cursor: Cursor[ByteBuffer]): (Array[Byte], Array[Byte]) = {
      val k = cursor.key()
      val v = cursor.`val`()
      val kArr = new Array[Byte](k.remaining())
      val vArr = new Array[Byte](v.remaining())
      k.get(kArr)
      v.get(vArr)
      kArr -> vArr
    }
  }

  def newRead(): Txn[ByteBuffer] = env.txnRead()

  def withWrite[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] = {
    val txn = env.txnWrite()
    f(txn).guarantee(Task {
      txn.commit()
      txn.close()
    })
  }

  def close(): Unit = {
    env.sync(true)
    dbi.close()
    val readers = env.readerCheck()
    assert(readers == 0, s"Active LMDB read txns still open: $readers")
    env.close()
  }
}