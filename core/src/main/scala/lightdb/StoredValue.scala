package lightdb

import fabric.rw._
import lightdb.id.{Id, StringId}
import lightdb.store.Store
import rapid.Task

case class StoredValue[T](key: String,
                          store: Store[KeyValue, KeyValue.type],
                          default: () => T,
                          persistence: Persistence)(implicit rw: RW[T]) { stored =>
  private lazy val id = Id[KeyValue](key)

  private var cached: Option[T] = None

  def get(): Task[T] = cached match {
    case Some(t) => Task.pure(t)
    case None if persistence == Persistence.Memory =>
      val t = default()
      cached = Some(t)
      Task.pure(t)
    case None => store.transaction { transaction =>
      transaction.get(id).map {
        case Some(kv) => kv.json.as[T]
        case None => default()
      }.map { t =>
        if (persistence != Persistence.Stored) {
          cached = Some(t)
        }
        t
      }
    }
  }

  def exists(): Task[Boolean] = store.transaction { transaction =>
    transaction.get(id).map(_.nonEmpty)
  }

  def set(value: T): Task[T] = if (persistence == Persistence.Memory) {
    cached = Some(value)
    Task.pure(value)
  } else {
    store.transaction { transaction =>
      transaction.upsert(KeyValue(id, value.asJson)).map { _ =>
        if (persistence != Persistence.Stored) {
          cached = Some(value)
        }
        value
      }
    }
  }

  def modify(f: T => T): Task[T] = Task {
    stored.synchronized {
      val current = get().sync()
      val modified = f(current)
      set(modified).sync()
    }
  }

  def clear(): Task[Unit] = store.transaction { transaction =>
    transaction.delete(_._id -> id).map {
      case true => cached = None
      case false => // Nothing
    }
  }
}