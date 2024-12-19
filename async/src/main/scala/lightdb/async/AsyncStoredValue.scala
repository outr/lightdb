package lightdb.async

import lightdb.StoredValue
import rapid.Task

case class AsyncStoredValue[T](underlying: StoredValue[T]) {
  def get: Task[T] = Task(underlying.get())
  def exists: Task[Boolean] = Task(underlying.exists())
  def set(value: T): Task[T] = Task(underlying.set(value))
  def clear(): Task[Unit] = Task(underlying.clear())
}
