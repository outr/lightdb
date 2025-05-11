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