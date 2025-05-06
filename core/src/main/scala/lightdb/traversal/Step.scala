package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

sealed trait Step[S <: Document[S], C <: Document[C]] {
  def run(ids: Set[Id[S]]): Task[Set[Id[C]]]
}

object Step {
  final case class Single[E <: Document[E], M <: DocumentModel[E], A <: Document[A], B <: Document[B]](step: GraphStep[E, M, A, B])
                                                                               (implicit tx: Transaction[E, M]) extends Step[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      Task.sequence(ids.toList.map(step.neighbors)).map(_.flatten.toSet)
  }

  final case class Chain[E <: Document[E], M <: DocumentModel[E], A <: Document[A], C <: Document[C], B <: Document[B]](prev: Step[A, C], next: GraphStep[E, M, C, B])
                                                                                                (implicit tx: Transaction[E, M]) extends Step[A, B] {
    override def run(ids: Set[Id[A]]): Task[Set[Id[B]]] =
      prev.run(ids).flatMap { mids =>
        Task.sequence(mids.toList.map(next.neighbors)).map(_.flatten.toSet)
      }
  }
}