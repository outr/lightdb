package lightdb.graph

import fabric.rw._
import lightdb.Id
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field.UniqueIndex

class EdgeConnectionsModel[Origin <: Document[Origin], From <: Document[From], To <: Document[To]] extends DocumentModel[EdgeConnections[Origin, From, To]] with JsonConversion[EdgeConnections[Origin, From, To]] {
  override implicit val rw: RW[EdgeConnections[Origin, From, To]] = RW.gen

  val connections: I[Set[Id[Origin]]] = field.index("connections", _.connections)
  val to: I[Set[Id[To]]] = field.index("to", _.to)
}
