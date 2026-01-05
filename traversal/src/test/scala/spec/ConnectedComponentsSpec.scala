package spec

import lightdb.traversal.graph.{ConnectedComponents, DisjointSet}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream}

@EmbeddedTest
class ConnectedComponentsSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "ConnectedComponents" should {
    "build components in-memory from an edge stream" in {
      val edges = Stream.emits(List(
        ("a", "b"),
        ("b", "c"),
        ("d", "e")
      ))

      for {
        ds <- ConnectedComponents.buildInMemory(edges)
        ra <- ds.find("a")
        rb <- ds.find("b")
        rc <- ds.find("c")
        rd <- ds.find("d")
        re <- ds.find("e")
      } yield {
        ra shouldBe rb
        rb shouldBe rc
        rd shouldBe re
        ra should not be rd
      }
    }

    "support a user-supplied DisjointSet implementation" in {
      val ds0: DisjointSet = DisjointSet.inMemory()
      val edges = Stream.emits(List(("x", "y"), ("y", "z")))

      for {
        ds <- ConnectedComponents.build(edges, ds0)
        rx <- ds.find("x")
        rz <- ds.find("z")
      } yield {
        ds shouldBe ds0
        rx shouldBe rz
      }
    }
  }
}

