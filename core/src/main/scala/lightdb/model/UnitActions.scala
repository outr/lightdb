package lightdb.model

import cats.effect.IO

class UnitActions {
  private var list = List.empty[() => IO[Unit]]

  def +=(listener: => IO[Unit]): Unit = add(listener)

  def add(listener: => IO[Unit]): Unit = synchronized {
    list = list ::: List(() => listener)
  }

  private[model] def invoke(): IO[Unit] = list.map(_()).sequence.map(_ => ())
}
