package lightdb.traversal

import lightdb.doc.Document
import lightdb.Id
import lightdb.transaction.Transaction
import rapid.Task

/** Represents a chain of traversal steps with enforced type transitions. */
sealed trait StepChain[Start <: Document[Start], End <: Document[End]] {
  def run(ids: Set[Id[Start]]): Task[Set[Id[End]]]
}

final case class SingleStep[
  Edge <: Document[Edge],
  A <: Document[A],
  B <: Document[B]
](
   step: GraphStep[Edge, A, B]
 )(implicit tx: Transaction[Edge]) extends StepChain[A, B] {
  override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] = {
    Task.sequence(ids.toList.map(id => step.neighbors(id))).map(_.flatten.toSet)
  }
}

final case class ChainStep[
  Edge <: Document[Edge],
  A <: Document[A],
  B <: Document[B],
  C <: Document[C]
](
   previous: StepChain[A, B],
   next: GraphStep[Edge, B, C]
 )(implicit tx: Transaction[Edge]) extends StepChain[A, C] {
  override def run(ids: Set[Id[A]]): Task[Set[Id[C]]] = {
    previous.run(ids).flatMap { midNodes =>
      Task.sequence(midNodes.toList.map(id => next.neighbors(id))).map(_.flatten.toSet)
    }
  }
}

/**
 * A traversal builder that allows chaining multiple steps and executing them type-safely.
 */
final class GraphTraversalEngine[Start <: Document[Start], Current <: Document[Current]](
                                                                                          val current: Set[Id[Start]],
                                                                                          val chain: StepChain[Start, Current]
                                                                                        ) {

  def step[Edge <: Document[Edge], Next <: Document[Next]](
                                                            via: GraphStep[Edge, Current, Next]
                                                          )(implicit tx: Transaction[Edge]): GraphTraversalEngine[Start, Next] =
    new GraphTraversalEngine(
      current = current,
      chain = ChainStep(chain, via)
    )

  def collectAllReachable(): Task[Set[Id[Current]]] =
    chain.run(current)
}

object GraphTraversalEngine {
  def start[Edge <: Document[Edge], Start <: Document[Start], Next <: Document[Next]](
                                                                                       startIds: Set[Id[Start]],
                                                                                       via: GraphStep[Edge, Start, Next]
                                                                                     )(implicit tx: Transaction[Edge]): GraphTraversalEngine[Start, Next] =
    new GraphTraversalEngine(
      current = startIds,
      chain = SingleStep(via)
    )
}