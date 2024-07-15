package lightdb.transaction

trait TransactionFeature {
  def commit(): Unit = {}

  def rollback(): Unit = {}

  def close(): Unit = {}
}
