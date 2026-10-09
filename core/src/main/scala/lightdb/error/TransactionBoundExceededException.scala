package lightdb.error

/**
 * Raised by the write that takes a group-commit transaction (or the group it joined) past the op bound declared when
 * the transaction was created. The transaction is marked rolled back, so it aborts and writes nothing; it never
 * flushes part of its writes to make room.
 */
case class TransactionBoundExceededException(storeName: String, count: Int, bound: Int)
  extends RuntimeException(s"Transaction on '$storeName' reached $count ops, past its group-commit bound of $bound; " +
    "the transaction aborts and writes nothing. Commit large writes in chunks.")
