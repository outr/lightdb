package lightdb.graph

import lightdb.Id
import lightdb.doc.Document

case class EdgeConnections[Origin <: Document[Origin], From <: Document[From], To <: Document[To]](_id: Id[EdgeConnections[Origin, From, To]],
                                                                                                   connections: Set[Id[Origin]],
                                                                                                   to: Set[Id[To]]) extends Document[EdgeConnections[Origin, From, To]]