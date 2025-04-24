package lightdb.traversal

import lightdb.Id
import lightdb.doc.Document
import lightdb.transaction.Transaction
import rapid.Task

import scala.collection.mutable

class GraphTraversalEngine[From <: Document[From], S](current: Set[Id[From]],
                                                      state: S,
                                                      visited: Set[Id[_]] = Set.empty,
                                                      depth: Int = 0) {
  def step[Edge <: Document[Edge], To <: Document[To]](via: GraphStep[Edge, From, To],
                                                       f: (Id[From], Int, S) => Task[TraversalDecision[S]])
                                                      (implicit transaction: Transaction[Edge]): GraphTraversalEngine[To, S] = {
    val next = mutable.Set.empty[Id[To]]
    var newState = state

    val tasks = current.toList.map { id =>
      for {
        decision <- f(id, depth, newState)
        neigh <- via.neighbors(id)
      } yield {
        decision match {
          case TraversalDecision.Continue(s) =>
            newState = s; next ++= neigh
          case TraversalDecision.Skip(s) => newState = s
          case TraversalDecision.Stop =>
        }
      }
    }

    Task.sequence(tasks)
      .map(_ =>
        new GraphTraversalEngine[To, S](
          next.toSet,
          newState,
          visited ++ current,
          depth + 1
        )
      )
      .sync()
  }

  def run(): Task[Set[Id[_]]] = Task.pure(visited ++ current)

  def fixpoint[Edge <: Document[Edge], To <: Document[To]](via: GraphStep[Edge, From, To],
                                                           f: (Id[From], Int, S) => Task[TraversalDecision[S]])
                                                          (implicit transaction: Transaction[Edge]): Task[Set[Id[_]]] = {
    // First, run the current engine to get the current set of nodes
    run().flatMap { currentNodes =>
      // Take one step in the traversal
      val nextEngine = step(via, f)

      // Run the next engine to get the next set of nodes
      nextEngine.run().flatMap { nextNodes =>
        // Check if we've discovered any new nodes
        val newNodes = nextNodes.diff(currentNodes)

        if (newNodes.isEmpty) {
          // No new nodes discovered, we've reached a fixpoint
          Task.pure(nextNodes)
        } else {
          // Continue traversal from the next engine
          nextEngine.fixpoint(
            via.asInstanceOf[GraphStep[Edge, To, To]],
            f.asInstanceOf[(Id[To], Int, S) => Task[TraversalDecision[S]]]
          )
        }
      }
    }
  }
}

object GraphTraversalEngine {
  def apply[From <: Document[From], S](start: Id[From], initial: S): GraphTraversalEngine[From, S] =
    new GraphTraversalEngine(Set(start), initial)
}