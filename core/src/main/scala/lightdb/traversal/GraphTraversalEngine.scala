package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.Task

/**
 * A graph traversal engine that chains steps to traverse a graph.
 *
 * @param current The current set of source IDs
 * @param chain The step chain to execute
 */
class GraphTraversalEngine[S <: Document[S], C <: Document[C]] private (private val current: Set[Id[S]],
                                                                        private val chain: GraphTraversalEngine.StepLike[S, C]) {
  /**
   * Create a BFS engine for traversing from the current nodes.
   */
  def bfs[E <: Document[E], M <: DocumentModel[E]](via: GraphStep[E, M, C, C],
                                                   maxDepth: Int = Int.MaxValue)
                                                  (implicit tx: PrefixScanningTransaction[E, M]): BFSEngine[C, E, M] =
    new BFSEngine(current.asInstanceOf[Set[Id[C]]], via, maxDepth)

  /**
   * Add another step to the traversal.
   */
  def step[E <: Document[E], M <: DocumentModel[E], Next <: Document[Next]](via: GraphStep[E, M, C, Next])
                                                                           (implicit tx: PrefixScanningTransaction[E, M]): GraphTraversalEngine[S, Next] =
    new GraphTraversalEngine(current, GraphTraversalEngine.ChainStep(chain, via))

  /**
   * Collect all nodes reachable with the current chain.
   */
  def collectAllReachable(): Task[Set[Id[C]]] =
    chain.run(current)
}

object GraphTraversalEngine {
  /**
   * A trait representing a step in the traversal.
   */
  sealed trait StepLike[S <: Document[S], C <: Document[C]] {
    def run(ids: Set[Id[S]]): Task[Set[Id[C]]]
  }

  /**
   * A single step using a GraphStep
   */
  final case class SingleStep[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](
                                                                                                            step: GraphStep[E, M, A, B]
                                                                                                          )(implicit tx: PrefixScanningTransaction[E, M]) extends StepLike[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      Task.sequence(ids.toList.map(id => step.neighbors(id, tx))).map(_.flatten.toSet)
  }

  /**
   * A chain of steps
   */
  final case class ChainStep[E <: Document[E], M <: DocumentModel[E], A <: Document[A], C <: Document[C], B <: Document[B]](
                                                                                                                             prev: StepLike[A, C],
                                                                                                                             next: GraphStep[E, M, C, B]
                                                                                                                           )(implicit tx: PrefixScanningTransaction[E, M]) extends StepLike[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      prev.run(ids).flatMap { mids =>
        Task.sequence(mids.toList.map(mid => next.neighbors(mid, tx))).map(_.flatten.toSet)
      }
  }

  /**
   * Start a traversal from a set of IDs.
   */
  def start[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](
                                                                                          startIds: Set[Id[A]],
                                                                                          via: GraphStep[E, M, A, B]
                                                                                        )(implicit tx: PrefixScanningTransaction[E, M]): GraphTraversalEngine[A, B] =
    new GraphTraversalEngine(startIds, SingleStep(via))

  /**
   * Start a traversal from a single ID.
   */
  def start[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](
                                                                                          startId: Id[A],
                                                                                          via: GraphStep[E, M, A, B]
                                                                                        )(implicit tx: PrefixScanningTransaction[E, M]): GraphTraversalEngine[A, B] =
    start(Set(startId), via)

  /**
   * Start a traversal from a TraversalBuilder.
   */
  def fromTraversalBuilder[A <: Document[A], B <: Document[B], E <: Document[E], M <: DocumentModel[E]](
                                                                                                         builder: TraversalBuilder[Id[A]],
                                                                                                         via: GraphStep[E, M, A, B]
                                                                                                       )(implicit tx: PrefixScanningTransaction[E, M]): GraphTraversalEngine[A, B] = {
    // Extract the root ID from the builder
    val rootId = builder match {
      case TraversalBuilder(Some(id), _, _, _, _, _) => id.asInstanceOf[Id[A]]
      case _ => throw new IllegalArgumentException("TraversalBuilder must have a root")
    }

    start(Set(rootId), via)
  }

  /**
   * Extension methods for TraversalBuilder
   */
  implicit class GraphTraversalBuilderOps[A <: Document[A]](builder: TraversalBuilder[Id[A]]) {
    /**
     * Convert this TraversalBuilder to a GraphTraversalEngine
     */
    def toGraphTraversal[E <: Document[E], M <: DocumentModel[E], B <: Document[B]](
                                                                                     via: GraphStep[E, M, A, B]
                                                                                   )(implicit tx: PrefixScanningTransaction[E, M]): GraphTraversalEngine[A, B] = {
      fromTraversalBuilder(builder, via)
    }
  }
}