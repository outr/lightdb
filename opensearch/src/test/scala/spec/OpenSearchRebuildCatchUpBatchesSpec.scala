package spec

import lightdb.opensearch.OpenSearchRebuild
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

class OpenSearchRebuildCatchUpBatchesSpec extends AnyWordSpec with Matchers {
  "OpenSearchRebuild.collectCatchUpBatches" should {
    "iterate batches until None and preserve nextToken chaining" in {
      final class CatchUp extends (Option[String] => Task[Option[OpenSearchRebuild.CatchUpBatch]]) {
        override def apply(token: Option[String]): Task[Option[OpenSearchRebuild.CatchUpBatch]] = Task {
          token match {
            case None =>
              Some(OpenSearchRebuild.CatchUpBatch(
                docs = List(OpenSearchRebuild.RebuildDoc("d1", fabric.obj("v" -> fabric.str("one")))),
                nextToken = Some("t1")
              ))
            case Some("t1") =>
              Some(OpenSearchRebuild.CatchUpBatch(
                docs = List(OpenSearchRebuild.RebuildDoc("d2", fabric.obj("v" -> fabric.str("two")))),
                nextToken = None
              ))
            case _ =>
              None
          }
        }
      }

      val batches = OpenSearchRebuild.collectCatchUpBatches(new CatchUp, maxBatches = 10).sync()
      batches.map(_.docs.map(_.id)) shouldBe List(List("d1"), List("d2"))
      batches.map(_.nextToken) shouldBe List(Some("t1"), None)
    }

    "fail when maxBatches is exceeded" in {
      final class InfiniteCatchUp extends (Option[String] => Task[Option[OpenSearchRebuild.CatchUpBatch]]) {
        override def apply(token: Option[String]): Task[Option[OpenSearchRebuild.CatchUpBatch]] = Task {
          Some(OpenSearchRebuild.CatchUpBatch(
            docs = Nil,
            nextToken = Some("t")
          ))
        }
      }

      val e = intercept[RuntimeException] {
        OpenSearchRebuild.collectCatchUpBatches(new InfiniteCatchUp, maxBatches = 2).sync()
      }
      e.getMessage should include("maxCatchUpBatches=2")
    }
  }
}


