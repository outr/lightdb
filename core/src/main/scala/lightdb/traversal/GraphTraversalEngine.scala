package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.Id
import lightdb.transaction.Transaction
import rapid.Task

class GraphTraversalEngine[S <: Document[S], C <: Document[C]] private (private val current: Set[Id[S]], private val chain: Step[S, C]) {
  def bfs[E <: Document[E], M <: DocumentModel[E]](via: GraphStep[E, M, C, C],
                                                   maxDepth: Int = Int.MaxValue)(implicit tx: Transaction[E, M]): BFSEngine[C, E, M] =
    new BFSEngine(current.asInstanceOf[Set[Id[C]]], via, maxDepth)

  def step[E <: Document[E], M <: DocumentModel[E], Next <: Document[Next]](via: GraphStep[E, M, C, Next])
                                                                           (implicit tx: Transaction[E, M]): GraphTraversalEngine[S, Next] =
    new GraphTraversalEngine(current, Step.Chain(chain, via))

  def collectAllReachable(): Task[Set[Id[C]]] =
    chain.run(current)
}

object GraphTraversalEngine {
  def start[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](startIds: Set[Id[A]], via: GraphStep[E, M, A, B])
                                                                                     (implicit tx: Transaction[E, M]): GraphTraversalEngine[A, B] =
    new GraphTraversalEngine(startIds, Step.Single(via))

  def start[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](startId: Id[A], via: GraphStep[E, M, A, B])
                                                                 (implicit tx: Transaction[E, M]): GraphTraversalEngine[A, B] =
    start(Set(startId), via)
}