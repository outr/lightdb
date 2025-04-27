package lightdb

import lightdb.doc.Document
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.Task

package object traversal {
  implicit class EdgeStoreExtras[E <: EdgeDocument[E, From, To], From <: Document[From], To   <: Document[To]](store: Store[E, EdgeModel[E, From, To]]) {
    def traverse(start: Id[From])(implicit tx: Transaction[E]): GraphTraversalEngine[From, To] = traverse(Set(start))

    def traverse(starts: Set[Id[From]])(implicit tx: Transaction[E]): GraphTraversalEngine[From, To] = {
      val m: EdgeModel[E, From, To] = store.model
      val step = new GraphStep[E, From, To] {
        override def neighbors(id: Id[From])
                              (implicit t: Transaction[E]): Task[Set[Id[To]]] = m.edgesFor(id)
      }
      GraphTraversalEngine.start[E, From, To](starts, step)
    }
  }
}
