package lightdb.store

import lightdb.doc.Document
import lightdb.id.Id

case class WriteBuffer[Doc <: Document[Doc]](map: Map[Id[Doc], WriteOp[Doc]] = Map.empty[Id[Doc], WriteOp[Doc]], delta: Int = 0)
