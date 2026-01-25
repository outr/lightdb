package lightdb.store

import lightdb.doc.Document
import lightdb.id.Id
import lightdb.store.write.WriteOp

import scala.collection.immutable.VectorMap

/**
 * Write buffer used by BufferedWritingTransaction.
 *
 * IMPORTANT: we use an insertion-ordered map (VectorMap) so flush order is deterministic.
 * Some backends (e.g. OpenSearch) can produce non-deterministic tie-break ordering for equal scores
 * if ingestion order is non-deterministic (HashMap iteration order).
 */
case class WriteBuffer[Doc <: Document[Doc]](
  map: Map[Id[Doc], WriteOp[Doc]] = VectorMap.empty[Id[Doc], WriteOp[Doc]],
  delta: Int = 0
)
