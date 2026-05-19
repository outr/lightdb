package lightdb.query

/**
 * One inner-hit returned alongside its outer hit.
 *
 * `value` is materialized at the call site via the caller's chosen `RW` (see
 * [[DocWithInnerHits.innerHitsFor]]). The caller picks a projection shape that fits the
 * fields the backend actually returned — typically a small case class containing only the
 * fields the consumer needs. This avoids the SplitStore pitfall where the backend `_source`
 * contains only indexed fields and a full case-class decode would always fail.
 *
 * `score` is the inner-hit's own relevance (independent of the outer hit). `highlights` is
 * per-field fragments when the inner-hits spec requested highlighting.
 */
final case class InnerHit[+V](value: V,
                              score: Double = 0.0,
                              highlights: Highlights = Highlights.empty)
