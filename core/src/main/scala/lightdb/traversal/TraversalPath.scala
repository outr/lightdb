package lightdb.traversal

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id

/**
 * A path in a traversal
 *
 * @param edges The edges that make up the path
 */
class TraversalPath[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](val edges: List[E]) {
  /**
   * Get the sequence of node IDs in the path
   */
  def nodes: List[Id[_]] = edges match {
    case Nil => Nil
    case _ => edges.head._from :: edges.map(_._to)
  }

  /**
   * Get the length of the path
   */
  def length: Int = edges.length
}

object TraversalPath {
  def apply[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                         edges: List[E]
                                                                                       ): TraversalPath[E, From, To] = new TraversalPath(edges)
}