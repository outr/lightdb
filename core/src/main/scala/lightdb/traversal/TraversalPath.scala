package lightdb.traversal

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id

case class TraversalPath[E <: EdgeDocument[E, From, From], From <: Document[From]](edges: List[E]) {
  def nodes: List[Id[From]] = edges match {
    case Nil => Nil
    case _ => edges.head._from :: edges.map(_._to)
  }
}