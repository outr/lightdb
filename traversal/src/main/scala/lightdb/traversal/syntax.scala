package lightdb.traversal

import lightdb.doc.{Document, DocumentModel}
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.PrefixScanningTransaction
import lightdb.traversal.graph.{EdgeTraversalBuilder, TraversalPath, TraversalStrategy}
import rapid.Stream

/**
 * Syntax / extension methods for enabling the lightweight graph traversal DSL against any
 * `PrefixScanningTransaction` without requiring `core` to depend on this module.
 */
object syntax {
  implicit final class PrefixScanningTransactionTraverseOps[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    private val tx: PrefixScanningTransaction[Doc, Model]
  ) extends AnyVal {
    def traverse: TransactionTraverse[Doc, Model] = new TransactionTraverse[Doc, Model](tx)
  }
}

/**
 * Per-transaction traversal helpers (exposed via `import lightdb.traversal.syntax.*`).
 */
final class TransactionTraverse[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  private val tx: PrefixScanningTransaction[Doc, Model]
) {
  /**
   * Get a stream of edges for the specified from ID.
   */
  def edgesFor[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
    fromId: Id[From]
  )(implicit ev: Doc =:= E): Stream[E] =
    tx.prefixStream(fromId.value).map[E](doc => ev(doc))

  /**
   * Start a traversal from a single document ID.
   */
  def from[D <: Document[D]](id: Id[D]): graph.DocumentTraversalBuilder[D] =
    _root_.lightdb.traversal.from(id)

  /**
   * Start a traversal from a set of document IDs.
   */
  def from[D <: Document[D]](ids: Set[Id[D]]): graph.DocumentTraversalBuilder[D] =
    _root_.lightdb.traversal.from(ids)

  /**
   * Find all edges reachable from a starting ID by following edges.
   */
  def reachableFrom[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
    from: Id[From],
    maxDepth: Int = Int.MaxValue
  )(implicit ev: From =:= To): Stream[E] = {
    _root_.lightdb.traversal
      .from(from)
      .withMaxDepth(maxDepth)
      .follow[E, To](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .edges
  }

  def allPaths[E <: EdgeDocument[E, From, From], From <: Document[From]](
    from: Id[From],
    to: Id[From],
    maxDepth: Int,
    bufferSize: Int = 100,
    edgeFilter: E => Boolean = (_: E) => true
  )(implicit ev: Doc =:= E): Stream[TraversalPath[E, From, From]] =
    _root_.lightdb.traversal
      .from(from)
      .withMaxDepth(maxDepth)
      .follow[E, From](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .findPaths(to)
      // A path is allowed iff every edge in it passes the filter; applying it here keeps the native
      // (unfiltered) traversal eligible while preserving edge-filter semantics.
      .filter(path => path.edges.forall(edgeFilter))

  /**
   * Find shortest paths between two nodes.
   */
  def shortestPaths[E <: EdgeDocument[E, From, To], From <: Document[From], To <: Document[To]](
    from: Id[From],
    to: Id[To],
    maxDepth: Int = Int.MaxValue,
    bufferSize: Int = 100,
    edgeFilter: E => Boolean = (_: E) => true
  )(implicit ev: Doc =:= E): Stream[TraversalPath[E, From, To]] =
    _root_.lightdb.traversal
      .from(from)
      .withMaxDepth(maxDepth)
      .follow[E, To](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .filter(edgeFilter)
      .findShortestPath(to)

  /**
   * Create a traversal for BFS with a single starting node.
   */
  def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startId: Id[N]
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startId)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.BFS)

  /**
   * Create a traversal for BFS with a single starting node and specified depth.
   */
  def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startId: Id[N],
    maxDepth: Int
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startId)
      .withMaxDepth(maxDepth)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.BFS)

  /**
   * Create a traversal for BFS with multiple starting nodes.
   */
  def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startIds: Set[Id[N]]
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startIds)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.BFS)

  /**
   * Create a traversal for BFS with multiple starting nodes and specified depth.
   */
  def bfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startIds: Set[Id[N]],
    maxDepth: Int
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startIds)
      .withMaxDepth(maxDepth)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.BFS)

  /**
   * Create a traversal for DFS with a single starting node.
   */
  def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startId: Id[N]
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startId)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.DFS)

  /**
   * Create a traversal for DFS with specified depth.
   */
  def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startId: Id[N],
    maxDepth: Int
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startId)
      .withMaxDepth(maxDepth)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.DFS)

  /**
   * Create a traversal for DFS with multiple starting nodes.
   */
  def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startIds: Set[Id[N]]
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startIds)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.DFS)

  /**
   * Create a traversal for DFS with multiple starting nodes and specified depth.
   */
  def dfs[E <: EdgeDocument[E, N, T], N <: Document[N], T <: Document[T]](
    startIds: Set[Id[N]],
    maxDepth: Int
  )(implicit ev: Doc =:= E): EdgeTraversalBuilder[E, N, T] =
    _root_.lightdb.traversal
      .from(startIds)
      .withMaxDepth(maxDepth)
      .follow[E, T](tx.asInstanceOf[PrefixScanningTransaction[E, _]])
      .using(TraversalStrategy.DFS)
}

