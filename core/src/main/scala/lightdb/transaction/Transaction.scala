package lightdb.transaction

import lightdb.Id
import lightdb.collection.Collection
import lightdb.document.Document
import lightdb.util.Unique

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class Transaction[D <: Document[D]](collection: Collection[D, _]) { transaction =>
  val id: String = Unique()

  private var map = Map.empty[TransactionKey[_], Any]
  private var locks = Set.empty[Id[D]]

  def lock(id: Id[D], delay: FiniteDuration = 100.millis): Unit = {
    Transaction.lock(id, this, delay)
    transaction.synchronized {
      locks += id
    }
  }

  def unlock(id: Id[D]): Unit = {
    Transaction.unlock(id, this)
    transaction.synchronized {
      locks -= id
    }
  }

  def withLock[Return](id: Id[D], delay: FiniteDuration = 100.millis)(f: => Return): Return = {
    lock(id, delay)
    try {
      f
    } finally {
      unlock(id)
    }
  }

  def mayLock[Return](id: Id[D],
                      establishLock: Boolean = true,
                      delay: FiniteDuration = 100.millis)(f: => Return): Return = if (establishLock) {
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

  def commit(): Unit = {
    collection.commit(this)
    locks.toList.foreach(unlock)
  }

  def rollback(): Unit = collection.rollback(this)
}

object Transaction {
  private lazy val locks = new ConcurrentHashMap[Id[_], Transaction[_]]

  @tailrec
  private def lock[D <: Document[D]](id: Id[D],
                                     transaction: Transaction[D],
                                     delay: FiniteDuration): Unit = {
    val existingTransaction = locks
      .compute(id, (_, currentTransaction) => {
        if (currentTransaction == null || currentTransaction == transaction) {
          transaction
        } else {
          currentTransaction
        }
      })
      if (existingTransaction != transaction) {
        Thread.sleep(delay.toMillis)
        lock[D](id, transaction, delay)
      }
  }

  private def unlock[D <: Document[D]](id: Id[D], transaction: Transaction[D]): Unit = locks
    .compute(id, (_, currentTransaction) => {
      if (currentTransaction == transaction) {
        null
      } else {
        currentTransaction
      }
    })
}