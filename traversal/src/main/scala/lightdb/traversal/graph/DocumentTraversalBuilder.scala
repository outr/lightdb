package lightdb.traversal.graph

import lightdb.doc.Document
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.Stream

/**
 * Builder for document traversals
 *
 * @param ids The stream of document IDs to start the traversal from
 * @param maxDepth The maximum depth to traverse. Defaults to Int.MaxValue
 */
case class DocumentTraversalBuilder[D <: Document[D]](ids: Stream[Id[D]],
                                                      maxDepth: Int = Int.MaxValue) {
  /**
   * Configure the maximum traversal depth
   */
  def withMaxDepth(depth: Int): DocumentTraversalBuilder[D] = copy(maxDepth = depth)

  /**
   * Follow edges to traverse the graph
   *
   * @tparam E The edge document type
   * @tparam T The target document type
   * @param tx A transaction that supports prefix scanning for the edge type
   * @return A builder for edge traversals
   */
  def follow[E <: EdgeDocument[E, D, T], T <: Document[T]](tx: PrefixScanningTransaction[E, _]): EdgeTraversalBuilder[E, D, T] =
    EdgeTraversalBuilder[E, D, T](ids, tx, maxDepth)

  /**
   * Get the stream of documents
   *
   * @param tx A transaction for retrieving documents
   * @return A stream of documents
   */
  def documents(tx: Transaction[D, _]): Stream[D] = ids.evalMap(id => tx(id))
}

