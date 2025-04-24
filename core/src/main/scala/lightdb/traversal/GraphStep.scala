package lightdb.traversal

import lightdb.{Id, LightDB}
import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.Task

/**
 * A simplified GraphStep trait that represents a way to find neighbors in a graph.
 * 
 * @tparam Edge The edge document type
 * @tparam From The source node document type
 * @tparam To The target node document type
 */
sealed trait GraphStep[Edge <: Document[Edge], From <: Document[From], To <: Document[To]] {
  /**
   * Find neighbors of a node in the graph.
   *
   * @param id The ID of the node to find neighbors for
   * @param transaction The transaction context
   * @return A task that resolves to a set of neighbor node IDs
   */
  def neighbors(id: Id[From])(implicit transaction: Transaction[Edge]): Task[Set[Id[To]]]
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
    From <: Document[From],
    To   <: Document[To]
  ](model: EdgeModel[Edge, From, To]): GraphStep[Edge, From, To] =
    new GraphStep[Edge, From, To] {
      override def neighbors(id: Id[From])(implicit transaction: Transaction[Edge]): Task[Set[Id[To]]] =
        model.edgesFor(id)
    }

  /**
   * Create a GraphStep that follows edges in the reverse direction (To → From).
   *
   * @param model The edge model
   * @return A GraphStep for reverse traversal
   */
  def reverse[
    Edge <: EdgeDocument[Edge, From, To],
    From <: Document[From],
    To   <: Document[To]
  ](model: EdgeModel[Edge, From, To]): GraphStep[Edge, To, From] =
    new GraphStep[Edge, To, From] {
      override def neighbors(id: Id[To])(implicit transaction: Transaction[Edge]): Task[Set[Id[From]]] =
        model.reverseEdgesFor(id)
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
    Node <: Document[Node]
  ](model: EdgeModel[Edge, Node, Node]): GraphStep[Edge, Node, Node] =
    new GraphStep[Edge, Node, Node] {
      override def neighbors(id: Id[Node])(implicit transaction: Transaction[Edge]): Task[Set[Id[Node]]] =
        for {
          fwd <- model.edgesFor(id)
          rev <- model.reverseEdgesFor(id)
        } yield fwd ++ rev
    }
}