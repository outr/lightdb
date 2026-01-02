package lightdb

/**
 * Public entry point for the lightweight graph traversal DSL (now implemented in the `traversal` module).
 *
 * Rationale:
 * - `core` should not depend on the `traversal` module.
 * - Downstream projects can standardize on `lightdb.traversal.*` as the primary entry point.
 */
package object traversal extends lightdb.traversal.graph.GraphTraversal {
  type DocumentTraversalBuilder[D <: lightdb.doc.Document[D]] = lightdb.traversal.graph.DocumentTraversalBuilder[D]
  val DocumentTraversalBuilder: lightdb.traversal.graph.DocumentTraversalBuilder.type = lightdb.traversal.graph.DocumentTraversalBuilder

  type EdgeTraversalBuilder[E <: lightdb.graph.EdgeDocument[E, F, T], F <: lightdb.doc.Document[F], T <: lightdb.doc.Document[T]] =
    lightdb.traversal.graph.EdgeTraversalBuilder[E, F, T]
  val EdgeTraversalBuilder: lightdb.traversal.graph.EdgeTraversalBuilder.type = lightdb.traversal.graph.EdgeTraversalBuilder

  type TraversalStrategy = lightdb.traversal.graph.TraversalStrategy
  val TraversalStrategy: lightdb.traversal.graph.TraversalStrategy.type = lightdb.traversal.graph.TraversalStrategy

  type TraversalPath[E <: lightdb.graph.EdgeDocument[E, From, To], From <: lightdb.doc.Document[From], To <: lightdb.doc.Document[To]] =
    lightdb.traversal.graph.TraversalPath[E, From, To]
  val TraversalPath: lightdb.traversal.graph.TraversalPath.type = lightdb.traversal.graph.TraversalPath
}

