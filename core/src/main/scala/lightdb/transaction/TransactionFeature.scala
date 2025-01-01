package lightdb.transaction

import rapid.Task

trait TransactionFeature {
  def commit(): Task[Unit] = Task.unit

  def rollback(): Task[Unit] = Task.unit

  def close(): Task[Unit] = Task.unit
}
