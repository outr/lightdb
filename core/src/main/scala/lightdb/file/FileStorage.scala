package lightdb.file

import fabric._
import fabric.rw._
import lightdb.{KeyValue, LightDB}
import lightdb.id.Id
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.transaction.PrefixScanningTransaction
import rapid.{Stream, Task}

import java.util.Base64

class FileStorage[S <: PrefixScanningStore[KeyValue, KeyValue.type]](val store: S) {
  private type TX = store.TX

  private val metaPrefix = "file:"
  private val dataPrefix = "data::"

  def put(fileName: String,
          data: Stream[Array[Byte]],
          chunkSize: Int): Task[FileMeta] = {
    require(chunkSize > 0, "chunkSize must be greater than zero")
    val fileId = Id[KeyValue]().value
    val now = System.currentTimeMillis()
    store.transaction { tx =>
      var totalBytes = 0L
      var totalChunks = 0

      def writeChunk(bytes: Array[Byte]): Task[Unit] = {
        val key = chunkKey(fileId, totalChunks)
        totalChunks += 1
        totalBytes += bytes.length
        tx.upsert(KeyValue(Id[KeyValue](key), encode(bytes))).map(_ => ())
      }

      data.evalMap { bytes =>
        Task.defer {
          split(bytes, chunkSize).foldLeft(Task.unit) { (acc, part) =>
            acc.next(writeChunk(part))
          }
        }
      }.drain.next {
        val meta = FileMeta(
          fileId = fileId,
          fileName = fileName,
          size = totalBytes,
          chunkSize = chunkSize,
          totalChunks = totalChunks,
          createdAt = now,
          updatedAt = now
        )
        tx.upsert(KeyValue(Id[KeyValue](metaKey(fileId)), meta.asJson)).map(_ => meta)
      }
    }
  }

  def get(fileId: String): Task[Option[FileMeta]] = store.transaction { tx =>
    tx.get(Id[KeyValue](metaKey(fileId))).map(_.map(kv => kv.json.as[FileMeta]))
  }

  def list: Task[List[FileMeta]] = store.transaction { tx =>
    tx.jsonPrefixStream(metaPrefix).map(toMeta).toList
  }

  def readAll(fileId: String): Task[List[Array[Byte]]] = stream(fileId)(_.toList)

  def stream[A](fileId: String)(f: Stream[Array[Byte]] => Task[A]): Task[A] = store.transaction { tx =>
    f(chunkStream(tx, fileId))
  }

  def delete(fileId: String): Task[Unit] = store.transaction { tx =>
    deleteChunks(tx, fileId).next(tx.delete(Id[KeyValue](metaKey(fileId))).map(_ => ())).unit
  }

  private def deleteChunks(tx: TX, fileId: String): Task[Int] =
    tx.jsonPrefixStream(chunkPrefix(fileId)).evalMap { json =>
      val kv = json.as[KeyValue]
      tx.delete(kv._id).map(deleted => if deleted then 1 else 0)
    }.toList.map(_.sum)

  private def chunkStream(tx: TX, fileId: String): Stream[Array[Byte]] =
    tx.jsonPrefixStream(chunkPrefix(fileId)).map(toChunkBytes)

  private def metaKey(fileId: String): String = s"$metaPrefix$fileId"

  private def chunkKey(fileId: String, index: Int): String = {
    val paddedIndex = f"$index%08d"
    s"$dataPrefix$fileId::$paddedIndex"
  }

  private def chunkPrefix(fileId: String): String = s"$dataPrefix$fileId::"

  private def toMeta(json: Json): FileMeta = json.as[KeyValue].json.as[FileMeta]

  private def toChunkBytes(json: Json): Array[Byte] = {
    val kv = json.as[KeyValue]
    decode(kv.json.asString)
  }

  private def encode(bytes: Array[Byte]): Json = str(Base64.getEncoder.encodeToString(bytes))

  private def decode(value: String): Array[Byte] = Base64.getDecoder.decode(value)

  private def split(bytes: Array[Byte], chunkSize: Int): List[Array[Byte]] = {
    if bytes.isEmpty then {
      Nil
    } else {
      val parts = scala.collection.mutable.ListBuffer.empty[Array[Byte]]
      var offset = 0
      while offset < bytes.length do {
        val end = math.min(offset + chunkSize, bytes.length)
        val chunk = new Array[Byte](end - offset)
        System.arraycopy(bytes, offset, chunk, 0, chunk.length)
        parts += chunk
        offset = end
      }
      parts.toList
    }
  }
}

object FileStorage {
  def apply[DB <: LightDB { type SM <: PrefixScanningStoreManager }](db: DB,
                                                                     storeName: String = "_files"): FileStorage[PrefixScanningStore[KeyValue, KeyValue.type]] = {
    val store: PrefixScanningStore[KeyValue, KeyValue.type] =
      db.storeCustom[KeyValue, KeyValue.type, db.SM](KeyValue, db.storeManager, Some(storeName))
    new FileStorage(store)
  }
}

