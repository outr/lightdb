package lightdb

import lightdb.doc.Document
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.Store

package object traversal {
  implicit class EdgeStoreExtras[From <: Document[From], To <: Document[To], Edge <: EdgeDocument[Edge, From, To], Model <: EdgeModel[Edge, From, To]](store: Store[Edge, Model]) {
    def traverse(startId: Id[From]): GraphTraversalEngine[From, Set[Id[From]]] =
      GraphTraversalEngine(startId)
  }
}
