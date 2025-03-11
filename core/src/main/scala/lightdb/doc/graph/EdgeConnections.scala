package lightdb.doc.graph

import lightdb.Id
import lightdb.doc.Document

case class EdgeConnections[From <: Document[From], To <: Document[To]](_id: Id[EdgeConnections[From, To]],
                                                                       connections: Set[Id[To]]) extends Document[EdgeConnections[From, To]]