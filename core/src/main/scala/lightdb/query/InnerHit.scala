package lightdb.query

/**
 * One inner-hit document returned alongside its outer hit.
 *
 * The `doc` is fully decoded via the child store's `RW`. `score` is the inner-hit's own
 * relevance (independent of the outer hit). `highlights` is per-field fragments when the
 * inner-hits spec requested highlighting.
 *
 * The `doc` type parameter is unbounded because the inner hits in a single result row may
 * span multiple relations with different child types. Use [[DocWithInnerHits.innerHitsFor]]
 * at the call site (where the relation's child type is known) to recover typed access.
 */
final case class InnerHit[+D](doc: D,
                              score: Double = 0.0,
                              highlights: Highlights = Highlights.empty)
