package lightdb

import lightdb.store.Store
import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument

package object traversal {
  implicit class TraversableStore[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    From <: Document[From],
    To <: Document[To]
  ](val store: Store[Edge, Model]) {
    def traversal(start: Id[From]): GraphTraversalEngineInstance[From] = GraphTraversalEngine.startFrom(start)
  }
}
