package lightdb.file

import fabric.rw.*

case class FileMeta(fileId: String,
                    fileName: String,
                    size: Long,
                    chunkSize: Int,
                    totalChunks: Int,
                    createdAt: Long,
                    updatedAt: Long)

object FileMeta {
  implicit val rw: RW[FileMeta] = RW.gen
}

