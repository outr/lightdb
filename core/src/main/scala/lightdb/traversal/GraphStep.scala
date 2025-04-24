package lightdb.traversal

import lightdb.{Id, LightDB}
import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.Task

import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
// 1) GraphStep: a typed “neighbor” function.
// ─────────────────────────────────────────────────────────────────────────────

trait GraphStep[
  Edge <: EdgeDocument[Edge, From, To],
  Model <: DocumentModel[Edge],
  From <: Document[From],
  To <: Document[To],
  A    <: Document[A],
  B    <: Document[B]
] {
  def neighbors(id: Id[A])(implicit transaction: Transaction[Edge]): Task[Set[Id[B]]]
}

object GraphStep {
  def forward[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    From <: Document[From],
    To   <: Document[To]
  ](model: EdgeModel[Edge, From, To]): GraphStep[Edge, Model, From, To, From, To] =
    new GraphStep[Edge, Model, From, To, From, To] {
      override def neighbors(id: Id[From])(implicit transaction: Transaction[Edge]): Task[Set[Id[To]]] =
        model.edgesFor(id)
    }

  /** Walk edges in the reverse direction (To → From). Requires From == To. */
  def reverse[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    From <: Document[From],
    To   <: Document[To]
  ](model: EdgeModel[Edge, From, To]): GraphStep[Edge, Model, From, To, To, From] =
    new GraphStep[Edge, Model, From, To, To, From] {
      override def neighbors(id: Id[To])(implicit transaction: Transaction[Edge]): Task[Set[Id[From]]] =
        model.reverseEdgesFor(id)
    }

  /** Walk edges both ways. Requires From == To. */
  def both[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    From <: Document[From],
    To   <: Document[To]
  ](model: EdgeModel[Edge, From, To])(implicit ev: Id[From] =:= Id[To]): GraphStep[Edge, Model, From, To, From, From] =
    new GraphStep[Edge, Model, From, To, From, From] {
      override def neighbors(id: Id[From])(implicit transaction: Transaction[Edge]): Task[Set[Id[From]]] =
        for {
          fwd <- model.edgesFor(id).map(_.map(to => ev.flip(to)))
          rev <- model.reverseEdgesFor(ev(id))
        } yield fwd ++ rev
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2) Traversal decisions
// ─────────────────────────────────────────────────────────────────────────────

sealed trait TraversalDecision[+S]
object TraversalDecision {
  case class Continue[S](newState: S) extends TraversalDecision[S]
  case class Skip[S](newState: S)     extends TraversalDecision[S]
  case object Stop                     extends TraversalDecision[Nothing]
}

// ─────────────────────────────────────────────────────────────────────────────
// 3) Traversal engine API
// ─────────────────────────────────────────────────────────────────────────────

trait GraphTraversalEngine[From <: Document[From]] {
  def startFrom(id: Id[From]): GraphTraversalEngineInstance[From]
}

trait GraphTraversalEngineInstance[From <: Document[From]] {
  def withState[S](initial: S): StatefulGraphTraversalEngine[From, S]
  def collect(): Task[Set[Id[From]]]
}

trait StatefulGraphTraversalEngine[From <: Document[From], S] {
  def step[
    Edge  <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    To    <: Document[To]
  ](
     via: GraphStep[Edge, Model, From, To, From, To],
     f:   (Id[From], Int, S) => Task[TraversalDecision[S]]
   )(implicit transaction: Transaction[Edge]): StatefulGraphTraversalEngine[To, S]

  def run(): Task[Set[Id[_]]]
}

// ─────────────────────────────────────────────────────────────────────────────
// 4) Internal implementation (don’t edit)
// ─────────────────────────────────────────────────────────────────────────────

object GraphTraversalEngine {
  def startFrom[From <: Document[From]](id: Id[From]) = new GraphTraversalEngineInstanceImpl(id)
}

private class GraphTraversalEngineInstanceImpl[From <: Document[From]](start: Id[From]) extends GraphTraversalEngineInstance[From] {
  override def withState[S](initial: S) =
    new StatefulGraphTraversalEngineImpl(Set(start), initial)

  override def collect(): Task[Set[Id[From]]] =
    Task.pure(Set(start))
}

private class StatefulGraphTraversalEngineImpl[From <: Document[From], S](
                                                                           current: Set[Id[From]],
                                                                           state:   S,
                                                                           visited: Set[Id[_]] = Set.empty,
                                                                           depth:   Int        = 0
                                                                         ) extends StatefulGraphTraversalEngine[From, S] {

  override def step[
    Edge  <: EdgeDocument[Edge, From, To],
    Model <: DocumentModel[Edge],
    To    <: Document[To]
  ](
     via: GraphStep[Edge, Model, From, To, From, To],
     f:   (Id[From], Int, S) => Task[TraversalDecision[S]]
   )(implicit transaction: Transaction[Edge]): StatefulGraphTraversalEngine[To, S] = {
    val next     = mutable.Set.empty[Id[To]]
    var newState = state

    val tasks = current.toList.map { id =>
      for {
        decision <- f(id, depth, newState)
        neigh    <- via.neighbors(id)
      } yield {
        decision match {
          case TraversalDecision.Continue(s) =>
            newState = s; next ++= neigh
          case TraversalDecision.Skip(s)     => newState = s
          case TraversalDecision.Stop        =>
        }
      }
    }

    Task.sequence(tasks)
      .map(_ =>
        new StatefulGraphTraversalEngineImpl(
          next.toSet,
          newState,
          visited ++ current,
          depth + 1
        )
      )
      .sync()
  }

  override def run() =
    Task.pure(visited ++ current)
}