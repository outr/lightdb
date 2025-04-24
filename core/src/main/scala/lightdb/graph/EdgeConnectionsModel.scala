package lightdb.graph

import fabric.rw._
import lightdb.Id
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field.UniqueIndex

case class EdgeConnectionsModel[From <: Document[From], To <: Document[To]]() extends DocumentModel[EdgeConnections[From, To]] with JsonConversion[EdgeConnections[From, To]] {
  override implicit val rw: RW[EdgeConnections[From, To]] = RW.gen

  val connections: UniqueIndex[EdgeConnections[From, To], Set[Id[To]]] = field.unique("connections", _.connections)
}
