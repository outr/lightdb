package lightdb.graph

import fabric.Json

/**
 * Optional capability a transaction can implement to provide a backend-native multi-hop graph
 * traversal (e.g. ArangoDB's AQL `FOR v,e IN 1..n OUTBOUND ...`). When a transaction implements this,
 * the `traversal` module's edge builder delegates whole-traversal `edges`/`targetIds` to it instead
 * of the generic per-node prefix-scan BFS/DFS.
 *
 * The returned stream yields each traversed edge as a LightDB-shaped edge document JSON (i.e. with
 * `_id`/`_from`/`_to` populated), ready to be read via the edge model's `RW`.
 */
trait NativeGraphTraversal {
  /**
   * Edges reachable from `startKeys` within `1..maxDepth` hops in the OUTBOUND direction.
   *
   * @param startKeys     starting node id values
   * @param maxDepth      maximum number of hops
   * @param breadthFirst  BFS when true, DFS when false
   */
  def nativeTraverseEdges(startKeys: List[String], maxDepth: Int, breadthFirst: Boolean): rapid.Stream[Json]
}
