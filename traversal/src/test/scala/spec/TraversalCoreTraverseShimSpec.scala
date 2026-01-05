package spec

import lightdb.doc.Document
import lightdb.id.Id
import lightdb.traversal.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

/**
 * Smoke-test for the lightweight traversal DSL.
 */
@EmbeddedTest
class TraversalCoreTraverseShimSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "lightdb.traversal" should {
    "expose GraphTraversal.from" in {
      val id = Id[P]("a")
      val builder = lightdb.traversal.from(id)
      builder.ids.toList.map { ids =>
        ids shouldBe List(id)
      }
    }

    "provide per-transaction traverse syntax (compile-time smoke test)" in {
      // If this syntax breaks, this test won't compile.
      trait DummyTx extends lightdb.transaction.PrefixScanningTransaction[P, P.type]
      val t: DummyTx = null.asInstanceOf[DummyTx]
      t.traverse
      succeed
    }
  }
}

