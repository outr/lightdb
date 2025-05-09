package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.Id
import lightdb.store.PrefixScanningStore
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import rapid.Task

/**
 * A GraphStep represents a way to find neighbors in a graph.
 *
 * @tparam Edge The edge document type
 * @tparam Model The model type for Edge
 * @tparam From The source node document type
 * @tparam To The target node document type
 */
trait GraphStep[Edge <: Document[Edge], Model <: DocumentModel[Edge], From <: Document[From], To <: Document[To]] {
  /**
   * Find neighbors of a node in the graph using an explicit transaction.
   *
   * @param id The ID of the node to find neighbors for
   * @param transaction The transaction to use (must be a PrefixScanningTransaction)
   * @return A task that resolves to a set of neighbor node IDs
   */
  def neighbors(id: Id[From], transaction: PrefixScanningTransaction[Edge, Model]): Task[Set[Id[To]]]
}

object GraphStep {
  /**
   * Create a GraphStep that follows edges in the forward direction (From → To).
   *
   * @param model The edge model
   * @return A GraphStep for forward traversal
   */
  def forward[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: EdgeModel[Edge, From, To],
    From <: Document[From],
    To   <: Document[To]
  ](model: Model): GraphStep[Edge, Model, From, To] =
    new GraphStep[Edge, Model, From, To] {
      override def neighbors(id: Id[From], transaction: PrefixScanningTransaction[Edge, Model]): Task[Set[Id[To]]] = {
        val typeEv = implicitly[Edge =:= Edge]
        transaction.traversal.edgesFor[Edge, From, To](id)(typeEv)
          .map(_._to)
          .toList
          .map(_.toSet)
      }
    }

  /**
   * Create a GraphStep that follows edges in the reverse direction (To → From).
   *
   * @param model The edge model
   * @return A GraphStep for reverse traversal
   */
  def reverse[
    Edge <: EdgeDocument[Edge, From, To],
    Model <: EdgeModel[Edge, From, To],
    From <: Document[From],
    To <: Document[To]
  ](model: Model): GraphStep[Edge, Model, To, From] =
    new GraphStep[Edge, Model, To, From] {
      override def neighbors(id: Id[To], transaction: PrefixScanningTransaction[Edge, Model]): Task[Set[Id[From]]] = {
        // For reverse traversal, we need to scan all documents and filter for matching _to
        val typeEv = implicitly[Edge =:= Edge]
        transaction.prefixStream("")
          .map(typeEv.apply)
          .filter(_._to == id)
          .map(_._from)
          .toList
          .map(_.toSet)
      }
    }

  /**
   * Create a GraphStep that follows edges in both directions.
   * Requires From and To as the same type.
   *
   * @param model The edge model
   * @return A GraphStep for bidirectional traversal
   */
  def both[
    Edge <: EdgeDocument[Edge, Node, Node],
    Model <: EdgeModel[Edge, Node, Node],
    Node <: Document[Node]
  ](model: Model): GraphStep[Edge, Model, Node, Node] =
    new GraphStep[Edge, Model, Node, Node] {
      override def neighbors(id: Id[Node], transaction: PrefixScanningTransaction[Edge, Model]): Task[Set[Id[Node]]] = {
        val typeEv = implicitly[Edge =:= Edge]

        // Forward direction - use edgesFor
        val forwardTask = transaction.traversal.edgesFor[Edge, Node, Node](id)(typeEv)
          .map(_._to)
          .toList
          .map(_.toSet)

        // Reverse direction - need to scan and filter
        val reverseTask = transaction.prefixStream("")
          .map(typeEv.apply)
          .filter(_._to == id)
          .map(_._from)
          .toList
          .map(_.toSet)

        // Combine forward and reverse results
        for {
          forward <- forwardTask
          reverse <- reverseTask
        } yield forward ++ reverse
      }
    }
}