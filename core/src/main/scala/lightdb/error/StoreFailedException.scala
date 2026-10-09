package lightdb.error

/**
 * Raised by a read of a store that has reported a fault it could not heal in process (a search index whose in-process
 * re-index failed, a journalled store whose committed intents could not be applied). Reads fail rather than serve
 * results with holes; reopening the database repairs the store.
 */
case class StoreFailedException(storeName: String, reason: String, cause: Throwable = null)
  extends RuntimeException(s"Store '$storeName' has failed and refuses reads until the database is reopened: $reason", cause)
