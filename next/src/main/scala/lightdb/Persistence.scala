package lightdb

sealed trait Persistence

object Persistence {
  /**
   * Stored on disk only
   */
  case object Stored extends Persistence

  /**
   * Stored on disk and cached in memory
   */
  case object Cached extends Persistence

  /**
   * Stored in memory only
   */
  case object Memory extends Persistence
}