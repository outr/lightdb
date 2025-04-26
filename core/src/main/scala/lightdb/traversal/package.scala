package lightdb

import lightdb.doc.Document
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.Store
import lightdb.transaction.Transaction

package object traversal {
  implicit class EdgeStoreExtras[
    From <: Document[From],
    To <: Document[To],
    Edge <: EdgeDocument[Edge, From, To],
    Model <: EdgeModel[Edge, From, To]
  ](store: Store[Edge, Model]) {
    def traverse(startId: Id[From])(implicit tx: Transaction[Edge]): GraphTraversalEngine[From, To] =
      traverse(Set(startId))

    def traverse(startIds: Set[Id[From]])(implicit tx: Transaction[Edge]): GraphTraversalEngine[From, To] =
      GraphTraversalEngine.start(
        startIds = startIds,
        via = GraphStep.forward(store.model)
      )
  }
}
