package lightdb

import cats.effect.IO
import fabric.rw._
import lightdb.model.Collection

case class StoredValue[T](key: String,
                          collection: Collection[KeyValue],
                          default: () => T,
                          cache: Boolean)(implicit rw: RW[T]) {
  private lazy val id = Id[KeyValue](key)

  private var cached: Option[T] = None

  def get(): IO[T] = cached match {
    case Some(t) => IO.pure(t)
    case None => collection.get(id).map {
      case Some(kv) => kv.value.as[T]
      case None => default()
    }.map { t =>
      if (cache) cached = Some(t)
      t
    }
  }

  def exists(): IO[Boolean] = collection.get(id).map(_.nonEmpty)

  def clear(): IO[Unit] = collection.delete(id).map { _ =>
    if (cache) cached = None
  }

  def set(value: T): IO[T] = collection
    .set(KeyValue(id, value.asJson))
    .map { _ =>
      if (cache) cached = Some(value)
      value
    }
}
