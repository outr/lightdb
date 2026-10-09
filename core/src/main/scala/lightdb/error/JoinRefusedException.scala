package lightdb.error

/** Raised when a transaction cannot join the transaction it was asked to join. */
case class JoinRefusedException(storeName: String, reason: String)
  extends RuntimeException(s"A transaction on '$storeName' cannot join: $reason")
