package lightdb

import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeByName
import fabric.rw._
import lightdb.collection.Collection

case class StoredValue[T](key: String,
                          collection: Collection[KeyValue, KeyValue.type],
                          default: () => T,
                          persistence: Persistence)(implicit rw: RW[T]) {
  private lazy val id = Id[KeyValue](key)

  private var cached: Option[T] = None

  def get(): IO[T] = cached match {
    case Some(t) => IO.pure(t)
    case None if persistence == Persistence.Memory =>
      val t = default()
      cached = Some(t)
      IO.pure(t)
    case None => collection.transaction { implicit transaction =>
      collection.get(id).map {
        case Some(kv) => kv.value.as[T]
        case None => default()
      }.map { t =>
        if (persistence != Persistence.Stored) {
          cached = Some(t)
        }
        t
      }
    }
  }

  def exists(): IO[Boolean] = collection.transaction { implicit transaction =>
    collection.get(id).map(_.nonEmpty)
  }

  def set(value: T): IO[T] = if (persistence == Persistence.Memory) {
    cached = Some(value)
    IO.pure(value)
  } else {
    collection.transaction { implicit transaction =>
      collection
        .set(KeyValue(id, value.asJson))
        .map { _ =>
          if (persistence != Persistence.Stored) {
            cached = Some(value)
          }
          value
        }
    }
  }

  def modify(f: T => IO[T]): IO[T] = collection.transaction { implicit transaction =>
    transaction.withLock(id) {
      if (persistence == Persistence.Memory) {
        get().flatMap(f).map { value =>
          cached = Some(value)
          value
        }
      } else {
        for {
          current <- get()
          modified <- f(current)
          _ <- set(modified).whenA(current != modified)
        } yield modified
      }
    }
  }

  def clear(): IO[Unit] = collection.transaction { implicit transaction =>
    collection.delete(id).map { _ =>
      cached = None
    }
  }
}