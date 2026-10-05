package spec

import lightdb.id.Id
import lightdb.lucene.LuceneDurability
import rapid.*

import java.nio.file.Path
import scala.concurrent.duration.DurationLong

/**
 * The process [[LuceneDeferredDurabilityCrashSpec]] kills: it opens a [[CrashDB]] with deferred durability and writes
 * until it is killed, printing `COMMITTED <i>` after each of its numbered transactions commits. Beside them one fiber
 * churns a small set of `c` documents and another searches.
 *
 * Arguments: database directory, deferred commit interval in milliseconds, number of the first transaction.
 */
object CrashWriter {
  def main(args: Array[String]): Unit = {
    val db = new CrashDB(Path.of(args(0)), LuceneDurability.Deferred(args(1).toLong.millis))
    val first = args(2).toInt
    db.init.sync()

    Task {
      var c = 0L
      while true do {
        c += 1
        val id = s"c${c % 40}"
        db.items.transaction { tx =>
          if c % 5 == 0 then tx.delete(Id[CrashItem](id)).unit
          else tx.upsert(CrashItem(s"c$c", id)).unit
        }.sync()
      }
    }.start()
    Task {
      while true do db.items.transaction(_.query.filter(_.text.words("text")).count).sync()
    }.start()

    var i = first
    while i < first + 1_000_000 do {
      if CrashItem.fails(i) then {
        db.items.transaction { tx =>
          tx.insert(CrashItem(s"f$i", s"f$i"))
            .next(tx.upsert(CrashItem(s"f$i-u", s"f${i - 7}")))
            .next(tx.flush)
            .next(Task.error[Unit](new RuntimeException("abort")))
        }.attempt.sync()
      } else {
        db.items.transaction { tx =>
          tx.insert(CrashItem(s"n$i", s"d$i"))
            .next(if CrashItem.fails(i - 1) then Task.unit else tx.upsert(CrashItem(s"n${i - 1}-u", s"d${i - 1}")).unit)
            .next(if CrashItem.deletes(i) then tx.delete(Id[CrashItem](s"d${i - 2}")).unit else Task.unit)
        }.sync()
        System.out.println(s"COMMITTED $i")
        System.out.flush()
      }
      i += 1
    }
  }
}
