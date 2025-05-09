package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.Task

/**
 * A TraversalStep represents a transformation from one set of document IDs to another.
 *
 * @tparam S The source document type
 * @tparam C The target document type
 */
sealed trait TraversalStep[S <: Document[S], C <: Document[C]] {
  /**
   * Run this step on a set of IDs.
   *
   * @param ids The source IDs
   * @return A Task containing the resulting IDs
   */
  def run(ids: Set[Id[S]]): Task[Set[Id[C]]]
}

object TraversalStep {
  /**
   * A single step that uses a GraphStep to transform IDs.
   */
  final case class Single[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](
                                                                                                        step: GraphStep[E, M, A, B]
                                                                                                      )(implicit tx: PrefixScanningTransaction[E, M]) extends TraversalStep[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      Task.sequence(ids.toList.map(id => step.neighbors(id, tx))).map(_.flatten.toSet)
  }

  /**
   * A chain of steps, where the output of the first step becomes the input to the second.
   */
  final case class Chain[E <: Document[E], M <: DocumentModel[E], A <: Document[A], C <: Document[C], B <: Document[B]](
                                                                                                                         prev: TraversalStep[A, C],
                                                                                                                         next: GraphStep[E, M, C, B]
                                                                                                                       )(implicit tx: PrefixScanningTransaction[E, M]) extends TraversalStep[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      prev.run(ids).flatMap { mids =>
        Task.sequence(mids.toList.map(mid => next.neighbors(mid, tx))).map(_.flatten.toSet)
      }
  }

  /**
   * Create a Step from a TraversalBuilder.
   */
  def fromTraversalBuilder[
    A <: Document[A],
    B <: Document[B],
    E <: Document[E],
    M <: DocumentModel[E]
  ](
     builder: TraversalBuilder[Id[A]],
     via: GraphStep[E, M, A, B]
   )(implicit tx: PrefixScanningTransaction[E, M]): TraversalStep[A, B] = {
    Single(via)
  }

  /**
   * Extension methods for TraversalBuilder to work with Steps
   */
  implicit class StepTraversalBuilderOps[A <: Document[A]](builder: TraversalBuilder[Id[A]]) {
    /**
     * Convert this TraversalBuilder to a Step
     */
    def toStep[E <: Document[E], M <: DocumentModel[E], B <: Document[B]](
                                                                           via: GraphStep[E, M, A, B]
                                                                         )(implicit tx: PrefixScanningTransaction[E, M]): TraversalStep[A, B] = {
      fromTraversalBuilder(builder, via)
    }

    /**
     * Chain this TraversalBuilder with another Step
     */
    def andThen[E <: Document[E], M <: DocumentModel[E], B <: Document[B], C <: Document[C]](
                                                                                              step: TraversalStep[B, C],
                                                                                              via: GraphStep[E, M, A, B]
                                                                                            )(implicit tx: PrefixScanningTransaction[E, M]): TraversalStep[A, C] = {
      val first = fromTraversalBuilder(builder, via)
      // Implementation would depend on your needs
      // For now, we'll just return the step
      step.asInstanceOf[TraversalStep[A, C]]
    }
  }
}