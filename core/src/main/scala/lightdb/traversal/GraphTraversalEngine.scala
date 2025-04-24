package lightdb.traversal

import lightdb.Id
import lightdb.doc.Document
import rapid.Task

case class GraphTraversalEngine[From <: Document[From]](start: Id[From]) {
  def withState[S](initial: S): StatefulGraphTraversalEngine[From, S] =
    new StatefulGraphTraversalEngine[From, S](Set(start), initial)

  def collect(): Task[Set[Id[From]]] =
    Task.pure(Set(start))
}
