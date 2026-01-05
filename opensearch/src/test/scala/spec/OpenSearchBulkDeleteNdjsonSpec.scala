package lightdb.opensearch

import fabric._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OpenSearchBulkDeleteNdjsonSpec extends AnyWordSpec with Matchers {
  "OpenSearchBulkRequest" should {
    "encode delete actions as single-line NDJSON entries (with optional routing)" in {
      val ops = List(
        OpenSearchBulkOp.delete(index = "idx", id = "d1", routing = None),
        OpenSearchBulkOp.delete(index = "idx", id = "d2", routing = Some("r2"))
      )

      val ndjson = OpenSearchBulkRequest(ops).toBulkNdjson

      ndjson should include("""{"delete":{"_index":"idx","_id":"d1"}}""")
      ndjson should include("""{"delete":{"_index":"idx","_id":"d2","routing":"r2"}}""")
      // each op must end with a newline
      ndjson.endsWith("\n") shouldBe true
      // delete ops should NOT include a source line after the meta line
      ndjson.split("\n").count(_.contains("\"delete\"")) shouldBe 2
    }
  }
}

