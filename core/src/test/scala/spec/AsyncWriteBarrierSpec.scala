package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.handler.AsyncWriteHandler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.*

@EmbeddedTest
class AsyncWriteBarrierSpec extends AnyWordSpec with Matchers {
  case class Row(_id: Id[Row] = Id[Row]("row")) extends Document[Row]
  object Row extends DocumentModel[Row] with JsonConversion[Row] { implicit val rw: RW[Row] = RW.gen }
  "Async transaction barrier" should {
    "reject zero workers instead of creating a barrier that cannot drain" in {
      intercept[IllegalArgumentException] {
        new AsyncWriteHandler[Row, Row.type](0, 1, 1.millis, 10, _ => Task.unit)
      }
    }
    "not report unflushed writes when another writer enqueues while the barrier is released" in {
      // Regression: `flush` is called per batch by `Transaction.upsert(stream)`; with concurrent
      // writers on one transaction the queue can refill right after the barrier passes. Live workers
      // will drain it — this must not fail as "workers exited with unflushed writes".
      val written = new java.util.concurrent.atomic.AtomicInteger(0)
      val handler = new AsyncWriteHandler[Row, Row.type](2, 1, 1.millis, 1000, ops => Task {
        Thread.sleep(1)
        written.addAndGet(ops.size)
        ()
      })
      try {
        val perWriter = 200
        val writers = (0 until 4).map { _ =>
          val t = new Thread(() => {
            (0 until perWriter).foreach { _ =>
              handler.write(WriteOp.Upsert(Row())).sync()
              Thread.sleep(0L, 200_000)
            }
          })
          t.start()
          t
        }
        var flushes = 0
        while (writers.exists(_.isAlive)) {
          handler.flush.sync() // must never throw while workers are alive
          flushes += 1
        }
        writers.foreach(_.join(5000L))
        handler.flush.sync()
        flushes should be > 0
        written.get() shouldBe 4 * perWriter
      } finally handler.close.sync()
    }
    List(false, true).foreach { abort =>
      s"wait for already-dequeued writes before ${if (abort) "rollback" else "commit"}" in {
        val entered = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val finished = new CountDownLatch(1)
        val handler = new AsyncWriteHandler[Row, Row.type](1, 1, 1.millis, 10, _ => Task {
          entered.countDown()
          require(release.await(2, TimeUnit.SECONDS))
        })
        try {
          handler.write(WriteOp.Upsert(Row())).sync()
          entered.await(2, TimeUnit.SECONDS) shouldBe true
          val barrier = (if (abort) handler.abort else handler.flush).map(_ => finished.countDown()).start.sync()
          try { finished.await(30, TimeUnit.MILLISECONDS) shouldBe false }
          finally release.countDown()
          finished.await(2, TimeUnit.SECONDS) shouldBe true
          barrier.join.sync()
          if (abort) intercept[IllegalArgumentException](handler.write(WriteOp.Upsert(Row())).sync())
        } finally { release.countDown(); handler.close.sync() }
      }
    }
  }
}
