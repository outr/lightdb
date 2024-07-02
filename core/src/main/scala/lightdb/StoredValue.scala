package lightdb

import fabric.rw._
import lightdb.collection.Collection

case class StoredValue[T](key: String,
                          collection: Collection[KeyValue, KeyValue.type],
                          default: () => T,
                          persistence: Persistence)(implicit rw: RW[T]) {
  private lazy val id = Id[KeyValue](key)

  private var cached: Option[T] = None

  def get(): T = cached match {
    case Some(t) => t
    case None if persistence == Persistence.Memory =>
      val t = default()
      cached = Some(t)
      t
    case None => collection.transaction { implicit transaction =>
      val t = collection.get(KeyValue._id, id) match {
        case Some(kv) => kv.value.as[T]
        case None => default()
      }
      if (persistence != Persistence.Stored) {
        cached = Some(t)
      }
      t
    }
  }

  def exists(): Boolean = collection.transaction { implicit transaction =>
    collection.get(KeyValue._id, id).nonEmpty
  }

  def set(value: T): T = if (persistence == Persistence.Memory) {
    cached = Some(value)
    value
  } else {
    collection.transaction { implicit transaction =>
      collection.set(KeyValue(id, value.asJson))
      if (persistence != Persistence.Stored) {
        cached = Some(value)
      }
      value
    }
  }

  def clear(): Unit = collection.transaction { implicit transaction =>
    if (collection.delete(KeyValue._id, id)) {
      cached = None
    }
  }
}