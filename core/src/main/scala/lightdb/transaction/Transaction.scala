package lightdb.transaction

import lightdb.Id
import lightdb.doc.Document

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class Transaction[Doc <: Document[Doc]] { transaction =>
  private var locks = Set.empty[Id[Doc]]

  private var map = Map.empty[TransactionKey[_], Any]

  def lock(id: Id[Doc], delay: FiniteDuration = 100.millis): Unit = {
    Transaction.lock(id, this, delay)
    transaction.synchronized {
      locks += id
    }
  }

  def unlock(id: Id[Doc]): Unit = {
    Transaction.unlock(id, this)
    transaction.synchronized {
      locks -= id
    }
  }

  def withLock[Return](id: Id[Doc], delay: FiniteDuration = 100.millis)
                      (f: => Return): Return = {
    lock(id, delay)
    try {
      f
    } finally {
      unlock(id)
    }
  }

  def mayLock[Return](id: Id[Doc],
                      establishLock: Boolean = true,
                      delay: FiniteDuration = 100.millis)
                     (f: => Return): Return = if (establishLock) {
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
    map.values.foreach {
      case f: TransactionFeature => f.commit()
      case _ => // Ignore
    }
  }

  def rollback(): Unit = {
    map.values.foreach {
      case f: TransactionFeature => f.rollback()
      case _ => // Ignore
    }
  }

  def close(): Unit = {
    map.values.foreach {
      case f: TransactionFeature => f.close()
      case _ => // Ignore
    }
    locks.foreach(unlock)
  }
}

object Transaction {
  private lazy val locks = new ConcurrentHashMap[Id[_], Transaction[_]]

  @tailrec
  private def lock[Doc <: Document[Doc]](id: Id[Doc],
                        transaction: Transaction[Doc],
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
      lock[Doc](id, transaction, delay)
    }
  }

  private def unlock[Doc <: Document[Doc]](id: Id[Doc], transaction: Transaction[Doc]): Unit = locks
    .compute(id, (_, currentTransaction) => {
      if (currentTransaction == transaction) {
        null
      } else {
        currentTransaction
      }
    })
}