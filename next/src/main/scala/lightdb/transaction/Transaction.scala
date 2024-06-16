package lightdb.transaction

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.document.Document

case class Transaction[D <: Document[D]](collection: Collection[D]) {
  private var map = Map.empty[TransactionKey[_], Any]

  def put[T](key: TransactionKey[T], value: T): Unit = synchronized {
    map += key -> value
  }

  def get[T](key: TransactionKey[T]): Option[T] = map.get(key)
    .map(_.asInstanceOf[T])

  def apply[T](key: TransactionKey[T]): T = get[T](key)
    .getOrElse(throw new RuntimeException(s"Key not found: $key. Keys: ${map.keys.mkString(", ")}"))

  def commit(): IO[Unit] = collection.commit(this)
  def rollback(): IO[Unit] = collection.rollback(this)
}

case class TransactionKey[T](value: String)