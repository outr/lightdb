package lightdb.traversal

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}

case class DocumentTraversal[Doc <: Document[Doc]](ids: rapid.Stream[Id[Doc]]) {
  def follow[E <: EdgeDocument[E, Doc, T], T <: Document[T]](tx: PrefixScanningTransaction[E, _]): EdgeTraversal[E, Doc, T] = {
    val edges = ids.flatMap(id => tx.traversal.edgesFor[E, Doc, T](id))
    EdgeTraversal(edges)
  }
}

case class EdgeTraversal[E <: EdgeDocument[E, F, T], F <: Document[F], T <: Document[T]](edges: rapid.Stream[E]) {
  def follow[E2 <: EdgeDocument[E2, T, T2], T2 <: Document[T2]](tx: PrefixScanningTransaction[E2, _]): EdgeTraversal[E2, T, T2] = {
    val nextEdges = edges.flatMap(edge => tx.traversal.edgesFor[E2, T, T2](edge._to))
    EdgeTraversal(nextEdges)
  }
  def documents(tx: Transaction[T, _]): rapid.Stream[T] = edges.evalMap(edge => tx(edge._to))
}