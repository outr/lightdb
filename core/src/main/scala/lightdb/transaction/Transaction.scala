package lightdb.transaction

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.Id
import lightdb.collection.Collection
import lightdb.document.Document

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class Transaction[D <: Document[D]](collection: Collection[D]) { transaction =>
  private var map = Map.empty[TransactionKey[_], Any]
  private var locks = Set.empty[Id[D]]

  def lock(id: Id[D], delay: FiniteDuration = 100.millis): IO[Unit] = Transaction.lock(id, this, delay).map { _ =>
    transaction.synchronized {
      locks += id
    }
  }

  def unlock(id: Id[D]): IO[Unit] = Transaction.unlock(id, this).map { _ =>
    transaction.synchronized {
      locks -= id
    }
  }

  def withLock[Return](id: Id[D], delay: FiniteDuration = 100.millis)(f: => IO[Return]): IO[Return] = lock(id, delay)
    .flatMap(_ => f)
    .guarantee(unlock(id))

  def mayLock[Return](id: Id[D],
                      establishLock: Boolean = true,
                      delay: FiniteDuration = 100.millis)(f: => IO[Return]): IO[Return] = if (establishLock) {
    withLock(id, delay)(f)
  } else {
    f
  }

  def put[T](key: TransactionKey[T], value: T): Unit = synchronized {
    map += key -> value
  }

  def get[T](key: TransactionKey[T]): Option[T] = map.get(key)
    .map(_.asInstanceOf[T])

  def getOrCreate[T](key: TransactionKey[T], create: => T): T = synchronized {
    get[T](key) match {
      case Some(t) => t
      case None =>
        val t: T = create
        put(key, t)
        t
    }
  }

  def apply[T](key: TransactionKey[T]): T = get[T](key)
    .getOrElse(throw new RuntimeException(s"Key not found: $key. Keys: ${map.keys.mkString(", ")}"))

  def commit(): IO[Unit] = collection.commit(this).flatMap { _ =>
    locks.toList.map(unlock).sequence.map(_ => ())
  }
  def rollback(): IO[Unit] = collection.rollback(this)
}

object Transaction {
  private lazy val locks = new ConcurrentHashMap[Id[_], Transaction[_]]

  private def lock[D <: Document[D]](id: Id[D],
                                     transaction: Transaction[D],
                                     delay: FiniteDuration): IO[Unit] = IO.blocking(locks
    .compute(id, (_, currentTransaction) => {
      if (currentTransaction == null || currentTransaction == transaction) {
        transaction
      } else {
        currentTransaction
      }
    })
  ).flatMap { existingTransaction =>
    if (existingTransaction == transaction) {
      IO.unit
    } else {
      IO.sleep(delay).flatMap(_ => lock[D](id, transaction, delay))
    }
  }

  private def unlock[D <: Document[D]](id: Id[D], transaction: Transaction[D]): IO[Unit] = IO.blocking(locks
    .compute(id, (_, currentTransaction) => {
      if (currentTransaction == transaction) {
        null
      } else {
        currentTransaction
      }
    })
  )
}