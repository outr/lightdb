package lightdb.traversal

import lightdb.Id
import lightdb.doc.Document
import lightdb.transaction.Transaction
import rapid.Task

sealed trait Step[S <: Document[S], C <: Document[C]] {
  def run(ids: Set[Id[S]]): Task[Set[Id[C]]]
}

object Step {
  final case class Single[E <: Document[E], A <: Document[A], B <: Document[B]](step: GraphStep[E, A, B])
                                                                               (implicit tx: Transaction[E]) extends Step[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      Task.sequence(ids.toList.map(step.neighbors)).map(_.flatten.toSet)
  }

  final case class Chain[E <: Document[E], A <: Document[A], M <: Document[M], B <: Document[B]](prev: Step[A, M], next: GraphStep[E, M, B])
                                                                                                (implicit tx: Transaction[E]) extends Step[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      prev.run(ids).flatMap { mids =>
        Task.sequence(mids.toList.map(next.neighbors)).map(_.flatten.toSet)
      }
  }
}