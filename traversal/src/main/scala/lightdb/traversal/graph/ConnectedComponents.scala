package lightdb.traversal.graph

import rapid.{Stream, Task}

/**
 * Convenience helpers for building connected components from an undirected edge stream.
 *
 * This is a generic graph primitive. Callers decide:
 * - what constitutes an edge
 * - whether to pre-filter pairs (fanout caps, scoring thresholds, etc.)
 */
object ConnectedComponents {
  /**
   * Build connected components by unioning all edges.
   *
   * Returns the same DisjointSet instance so callers can perform subsequent `find` queries on it.
   */
  def build(edges: Stream[(String, String)], ds: DisjointSet): Task[DisjointSet] = {
    edges.evalMap { case (a, b) => ds.union(a, b) }.drain.map(_ => ds)
  }

  /**
   * Build connected components in-memory.
   */
  def buildInMemory(edges: Stream[(String, String)]): Task[DisjointSet] =
    build(edges, DisjointSet.inMemory())
}

