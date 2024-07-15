package lightdb.async

import cats.effect.IO
import lightdb.StoredValue

case class AsyncStoredValue[T](underlying: StoredValue[T]) {
  def get: IO[T] = IO.blocking(underlying.get())
  def exists: IO[Boolean] = IO.blocking(underlying.exists())
  def set(value: T): IO[T] = IO.blocking(underlying.set(value))
  def clear(): IO[Unit] = IO.blocking(underlying.clear())
}
