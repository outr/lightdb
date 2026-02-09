package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.handler.AsyncWriteHandler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}
import scala.util.*

import scala.concurrent.duration.DurationInt

@EmbeddedTest
class AsyncWriteHandlerErrorPropagationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Doc(value: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  "AsyncWriteHandler" should {
    "propagate background flush errors to close" in {
      val error = new RuntimeException("boom")
      val handler = new AsyncWriteHandler[Doc, Doc.type](
        activeThreads = 1,
        chunkSize = 1,
        waitTime = 10.millis,
        maxQueueSize0 = 100,
        flushOps = _ => Task.error(error)
      )

      val op = WriteOp.Upsert(Doc("value", Id[Doc]("d1")))
      handler
        .write(op)
        .next(Task.sleep(50.millis))
        .next(handler.close.attempt)
        .map {
          case Failure(t) => t.getMessage should include("boom")
          case Success(_) => fail("Expected AsyncWriteHandler to surface background error")
        }
    }
  }
}
