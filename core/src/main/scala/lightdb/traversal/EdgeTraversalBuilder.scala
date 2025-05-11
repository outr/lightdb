package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import rapid.{Stream, Task}

/**
 * Specialized builder for edge traversal
 */
class EdgeTraversalBuilder[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
                                                                                                        startId: Id[From],
                                                                                                        transaction: PrefixScanningTransaction[E, _]
                                                                                                      ) {
  /**
   * Follow edges using the provided step
   */
  def via[M <: DocumentModel[E]](
                                  step: GraphStep[E, M, From, To]
                                ): Task[Seq[Id[To]]] = {
    step.neighbors(startId, transaction.asInstanceOf[PrefixScanningTransaction[E, M]])
      .map(_.toSeq)
  }

  /**
   * Get a stream of results
   */
  def stream: Stream[Id[To]] = {
    // Use prefixStream directly for efficiency
    transaction.prefixStream(startId.value)
      .filter(_._from == startId)
      .map(_._to)
  }

  /**
   * Get all results as a list
   */
  def toList: Task[List[Id[To]]] = {
    stream.toList
  }
}
