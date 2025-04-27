package lightdb.traversal

sealed trait TraversalDecision[+S]

object TraversalDecision {
  case class Continue[S](newState: S) extends TraversalDecision[S]
  case class Skip[S](newState: S) extends TraversalDecision[S]
  case object Stop extends TraversalDecision[Nothing]
}