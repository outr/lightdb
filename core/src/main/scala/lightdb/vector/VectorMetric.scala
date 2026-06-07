package lightdb.vector

/**
 * Similarity metric for vector / embedding nearest-neighbor (KNN) search.
 *
 * Each metric defines a *distance* where a smaller value means more similar, so an ascending
 * [[lightdb.Sort.ByVectorDistance]] returns the nearest neighbors first. Backends map the metric to
 * their native operator/index (e.g. pgvector's `<=>` / `<->` / `<#>` and matching HNSW operator
 * class). Vector search is a native-only capability — there is intentionally no in-memory emulation.
 */
sealed trait VectorMetric

object VectorMetric {
  /** Cosine distance (`1 - cosine_similarity`); range [0, 2], 0 = identical direction. pgvector `<=>`. */
  case object Cosine extends VectorMetric

  /** Euclidean (L2) distance. pgvector `<->`. */
  case object Euclidean extends VectorMetric

  /**
   * Negative inner product, so that a larger dot product (more similar) yields a smaller distance and
   * sorts first under ascending order. Mirrors pgvector `<#>` (which also returns the negative IP).
   */
  case object DotProduct extends VectorMetric
}
