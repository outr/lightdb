package lightdb.view

import scala.concurrent.duration.FiniteDuration

/**
 * How a [[View]] is stored and kept current. The strategy is a tuning knob over the same view
 * definition — you can change it without touching the query or its readers.
 *
 *  - [[OnDemand]]: virtual, no storage; reads run the relation live (always fresh).
 *  - [[Cached]]: materialized; refreshed per [[RefreshPolicy]] (bounded staleness, fast reads).
 *  - [[Triggered]]: materialized; incrementally maintained as dependency transactions commit
 *    (fast reads, effectively always consistent). Atomic where the backend supports a shared
 *    transaction, synchronous-immediately-after otherwise.
 */
enum Materialization {
  case OnDemand
  case Cached(refresh: RefreshPolicy)
  case Triggered
}

object Materialization {
  /** Materialized, rebuilt only when `reBuild` is called. */
  val cachedManual: Materialization = Cached(RefreshPolicy.Manual)
}

/** When a [[Materialization.Cached]] view is rebuilt. */
enum RefreshPolicy {
  /** Only when `reBuild` is called explicitly. */
  case Manual
  /** Rebuilt on a background interval. */
  case Every(interval: FiniteDuration)
  /** Rebuilt lazily on read when the cache is older than `maxAge`. */
  case StaleAfter(maxAge: FiniteDuration)
}
