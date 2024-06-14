package lightdb.util

import cats.effect._

object LazyIO {
  def apply[T](io: IO[T]): IO[T] = io.memoize.unsafeRunSync()(cats.effect.unsafe.implicits.global)
}