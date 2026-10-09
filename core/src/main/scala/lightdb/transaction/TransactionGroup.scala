package lightdb.transaction

import lightdb.error.TransactionBoundExceededException
import rapid.Task

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.jdk.CollectionConverters.*

/**
 * The transactions that commit or roll back together: an owner and the transactions joined to it
 * (`store.transaction.joining(owner)`). The owner's commit decides every member's writes.
 *
 * A joined transaction is released (staged) before its owner commits: its writes wait in the group, and its
 * `transactionCommitted` or `transactionRolledBack` runs when the owner's outcome is known, before the owner's own.
 *
 * `bound` is the op bound declared when the owner was created (a group-commit transaction). Every write of every
 * member counts against it; the write that crosses it fails loud and the whole group aborts.
 */
final class TransactionGroup private[lightdb] (val owner: Transaction[?, ?], val bound: Option[Int]) {
  private val writes = new AtomicInteger(0)
  private val openJoins = new AtomicInteger(0)
  private val members = new ConcurrentLinkedQueue[TransactionGroup.Member]
  private val completed = new AtomicBoolean(false)

  /** Bytes a backend has reserved for the group's writes, for a backend whose commit must fit a size (a journal). */
  private[lightdb] val reservedBytes = new AtomicLong(0L)

  /** Writes counted against the bound so far, across the owner and its members. */
  def writeCount: Int = writes.get()

  /** Joined transactions that have been staged and wait for the owner's outcome. */
  def staged: List[Transaction[?, ?]] = members.asScala.toList.map(_.tx)

  /** Joined transactions created and not yet released or aborted. */
  def openMembers: Int = openJoins.get()

  private[lightdb] def countWrite(tx: Transaction[?, ?]): Unit = bound.foreach { b =>
    val n = writes.incrementAndGet()
    if n > b then {
      val error = TransactionBoundExceededException(tx.store.name, n, b)
      tx.rollBackFor(error)
      owner.rollBackFor(error)
      throw error
    }
  }

  private[lightdb] def joinOpened(): Unit = openJoins.incrementAndGet()

  private[lightdb] def joinClosed(): Unit = openJoins.decrementAndGet()

  private[lightdb] def stage(tx: Transaction[?, ?], outcome: Boolean => Task[Unit]): Unit = {
    members.add(new TransactionGroup.Member(tx, outcome))
    openJoins.decrementAndGet()
  }

  /** Runs every staged member's outcome once: `committed` when the owner committed, otherwise rolled back. */
  private[lightdb] def complete(committed: Boolean): Task[Unit] = Task.defer {
    if completed.compareAndSet(false, true) then {
      val list = members.asScala.toList
      list.foldLeft(Task.unit) { (task, member) =>
        val tx = member.tx
        val outcome = member.outcome
        task.guarantee(Task.defer {
          if !committed then tx.markRolledBack()
          outcome(committed)
        })
      }
    } else Task.unit
  }
}

object TransactionGroup {
  private[transaction] final class Member(val tx: Transaction[?, ?], val outcome: Boolean => Task[Unit])
}
