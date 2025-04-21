package lightdb.store

import lightdb.Id
import lightdb.doc.Document

case class WriteBuffer[Doc <: Document[Doc]](map: Map[Id[Doc], WriteOp[Doc]] = Map.empty[Id[Doc], WriteOp[Doc]], delta: Int = 0)
