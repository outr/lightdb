package lightdb.error

/** Raised when a store, or the native resource behind it, is used after it has been disposed. */
case class StoreDisposedException(storeName: String) extends RuntimeException(s"Store $storeName is disposed")
