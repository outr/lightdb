package spec

import lightdb.id.Id
import lightdb.store.journal.{Journal, JournalStep}
import rapid.*

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The process [[JournalCrashSpec]] kills: it opens a [[JournalCrashDB]], seeds it, then halts itself (no shutdown hook,
 * no disposal: what a kill leaves) at one step of the next journalled commit.
 *
 * Arguments: database directory, backend, scenario, step.
 *  - `group`: one commit of an owner (two inserts, a delete of a seeded row) and the transactions joined to it (a tag,
 *    a link and, through the link's reverse-edge trigger, its reverse row), halted at `step`.
 *  - `order`: two concurrent commits upserting one id ("first", then "second"), halted before either is applied.
 *  - `open`: only opens the database, halted at `step` of the replay.
 */
object JournalCrashWriter {
  val Exit: Int = 77

  private def matches(step: JournalStep, name: String, firstStore: Option[String]): Boolean = (step, name) match {
    case (_: JournalStep.BeforeIntent, "BeforeIntent") => true
    case (_: JournalStep.AfterIntent, "AfterIntent") => true
    case (_: JournalStep.BeforeApply, "BeforeApply") => true
    case (JournalStep.AfterApplyStore(_, store), "AfterApplyStore") => firstStore.forall(_ == store)
    case (_: JournalStep.BeforeDelete, "BeforeDelete") => true
    case (_: JournalStep.AfterDelete, "AfterDelete") => true
    case _ => false
  }

  private def halt(): Nothing = {
    System.out.println("HALT")
    System.out.flush()
    Runtime.getRuntime.halt(Exit)
    throw new IllegalStateException("unreachable")
  }

  def main(args: Array[String]): Unit = {
    val dir = Path.of(args(0))
    val backend = args(1)
    val scenario = args(2)
    val stepName = args(3)
    val armed = new AtomicBoolean(scenario == "open")
    if scenario == "open" then Journal.faultHook = step => if armed.get() && matches(step, stepName, None) then halt()

    val db = new JournalCrashDB(dir, backend)
    db.init.sync()

    scenario match {
      case "group" =>
        db.people.transaction(_.insert(CrashPerson("seeded", Id("seeded")))).sync()
        Journal.faultHook = step => if armed.get() && matches(step, stepName, Some(db.people.name)) then halt()
        armed.set(true)
        db.people.transaction { owner =>
          owner.insert(CrashPerson("one", Id("p1")))
            .next(owner.insert(CrashPerson("two", Id("p2"))))
            .next(owner.delete(Id[CrashPerson]("seeded")))
            .next(db.tags.transaction.joining(owner)(_.insert(CrashTag("tag", Id("t1")))))
            .next(db.links.transaction.joining(owner)(_.insert(CrashLink(Id[CrashPerson]("p1"), Id[CrashPerson]("p2")))))
            .unit
        }.sync()
      case "order" =>
        val secondIntent = new CountDownLatch(1)
        val firstApply = new AtomicBoolean(true)
        Journal.faultHook = {
          case JournalStep.AfterIntent(_) if armed.get() && !firstApply.get() => secondIntent.countDown()
          case JournalStep.BeforeApply(_, _) if armed.get() && firstApply.compareAndSet(true, false) =>
            secondIntent.await()
            halt()
          case _ => ()
        }
        armed.set(true)
        val first = new Thread(() => { db.people.transaction(_.upsert(CrashPerson("first", Id("same")))).sync(); () })
        first.start()
        Thread.sleep(500)
        db.people.transaction(_.upsert(CrashPerson("second", Id("same")))).sync()
      case "open" => ()
      case other => throw new IllegalArgumentException(s"unknown scenario $other")
    }
    System.out.println("FINISHED")
    System.out.flush()
    Runtime.getRuntime.halt(0)
  }
}
