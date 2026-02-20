package spec

import lightdb.traversal.graph.ConnectedComponents
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream}

@EmbeddedTest
class TraversalConnectedComponentsPerformanceSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private val EdgeCount = 200_000
  private val MaxElapsedMs = 20_000L

  "ConnectedComponents performance" should {
    "process a large chain within baseline budget" in {
      val edges = Stream.emits((0 until EdgeCount).iterator.map(i => s"n$i" -> s"n${i + 1}").toList)
      val started = System.currentTimeMillis()
      for {
        ds <- ConnectedComponents.buildInMemory(edges)
        first <- ds.find("n0")
        last <- ds.find(s"n$EdgeCount")
      } yield {
        val elapsed = System.currentTimeMillis() - started
        withClue(s"elapsedMs=$elapsed edgeCount=$EdgeCount maxElapsedMs=$MaxElapsedMs") {
          elapsed should be <= MaxElapsedMs
        }
        first shouldBe last
      }
    }
  }
}

