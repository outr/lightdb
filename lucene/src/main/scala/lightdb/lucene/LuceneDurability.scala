package lightdb.lucene

import fabric.rw.*
import profig.Profig

import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * When a Lucene store's committed changes become durable.
 *
 * Either way a committed change is visible at once: every transaction searches through a near-real-time reader over
 * the index writer, so visibility never waited for a durable commit. Durability is the flush, the new commit point
 * and the fsync of every new segment file, the costliest step of a write.
 */
enum LuceneDurability {
  /** Every transaction that changed the index commits it durably before its own commit returns. */
  case Immediate

  /**
   * A transaction's commit makes its changes visible and leaves the durable commit to a timer: one Lucene commit,
   * `interval` after the first change since the last one, covers every change made meanwhile.
   *
   * Only for an index that mirrors a storage store (`StoreMode.Indexes`, as in a split collection) and lives on disk:
   * it can be rebuilt from that storage. While changes are not yet durable a marker file sits in the index directory
   * (written, and fsynced, before the first change reaches storage); an index opened with the marker present was
   * left behind by a crash and is rebuilt from its storage before use. Any other Lucene store commits immediately.
   */
  case Deferred(interval: FiniteDuration = LuceneDurability.DefaultInterval)
}

object LuceneDurability {
  val DefaultInterval: FiniteDuration = 1_000L.millis

  /**
   * The durability configured for Lucene stores that are not given one explicitly: `lightdb.lucene.durability`
   * (`immediate`, the default, or `deferred`) with `lightdb.lucene.durableCommitIntervalMs` for the interval.
   */
  def configured: LuceneDurability = Profig("lightdb.lucene.durability").opt[String].map(_.trim.toLowerCase) match {
    case None | Some("immediate") => Immediate
    case Some("deferred") =>
      Deferred(Profig("lightdb.lucene.durableCommitIntervalMs").opt[Long].map(_.millis).getOrElse(DefaultInterval))
    case Some(other) =>
      throw new IllegalArgumentException(s"lightdb.lucene.durability must be immediate or deferred, not '$other'")
  }
}
