package lightdb.transaction

import lightdb.Id
import lightdb.doc.Document

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait Transaction[Doc <: Document[Doc]] { transaction =>
  private var locks = Set.empty[Id[Doc]]

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

  def commit(): Unit

  def rollback(): Unit
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