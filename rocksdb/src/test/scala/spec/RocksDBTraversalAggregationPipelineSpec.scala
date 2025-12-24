package spec

import lightdb.traversal.pipeline.{Accumulator, Pipeline, Stage}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream}

/**
 * Pure pipeline tests, hosted in rocksdb module so all runnable specs are colocated with a concrete store module.
 */
@EmbeddedTest
class RocksDBTraversalAggregationPipelineSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "RocksDBTraversalAggregationPipelineSpec" should {
    "support typed match -> project -> groupBy" in {
      val p = Pipeline.from(Stream.emits(List(1, 2, 3, 4, 5, 6)))
        .filter(_ % 2 == 0)
        .map(n => (if (n <= 3) "low" else "high") -> n.toDouble)
        .pipe(Stage.groupBy[(String, Double), String, Double, Double](_._1, Accumulator.sum(_._2)))

      p.stream.toList.map { rows =>
        rows.toMap shouldBe Map("low" -> 2.0, "high" -> 10.0) // 2 + (4+6)
      }
    }

    "support unwind + sort + skip/limit" in {
      val p = Pipeline.from(Stream.emits(List(
        ("a", List(3, 1)),
        ("b", List(2))
      )))
        .pipe(Stage.unwind[(String, List[Int]), Int](_._2))
        .pipe(Stage.sortBy(identity))
        .pipe(Stage.skip(1))
        .pipe(Stage.limit(2))

      p.stream.toList.map { list =>
        list shouldBe List(2, 3)
      }
    }

    "support bounded sortByPage for pagination without full materialization" in {
      val input = List(9, 1, 5, 3, 2, 8, 4, 7, 6)
      val p = Pipeline.from(Stream.emits(input))
        .pipe(Stage.sortByPage[Int, Int](identity, offset = 2, limit = 3))

      // Expect sorted asc: 1,2,3,4,5,6,7,8,9 -> page(offset=2,limit=3) == 3,4,5
      p.stream.toList.map { list =>
        list shouldBe List(3, 4, 5)
      }
    }

    "support facetTop (top-N counts) as a streaming facet-like reducer" in {
      val input = List("b", "a", "a", "c", "b", "a", "d", "b")
      val p = Pipeline.from(Stream.emits(input))
        .pipe(Stage.facetTop[String, String](identity, limit = 3))

      // counts: a=3, b=3, c=1, d=1. top3 should be (a,3),(b,3),(c,1) with tie-break by key asc.
      p.stream.toList.map { list =>
        list shouldBe List("a" -> 3L, "b" -> 3L, "c" -> 1L)
      }
    }

    "support reduce2(count, facetTop) to compute multiple reducer outputs in one pass" in {
      val input = List("b", "a", "a", "c", "b", "a", "d", "b")
      val p = Pipeline.from(Stream.emits(input))
        .pipe(Stage.reduce2(
          Accumulator.count[String],
          Accumulator.facetTop[String, String](identity, limit = 2)
        ))

      p.stream.toList.map { list =>
        list shouldBe List(8L -> List("a" -> 3L, "b" -> 3L))
      }
    }

    "support groupBy2 (two accumulators) and facet2" in {
      val base = Pipeline.from(Stream.emits(List("a", "a", "b", "c", "c", "c")))
      val grouped = base.pipe(Stage.groupBy2[String, String, Long, Long, Double, Double](
        key = identity,
        a1 = Accumulator.count,
        a2 = Accumulator.sum(_ => 1.0)
      ))

      val faceted = base.pipe(Stage.facet2(Stage.count, Stage.groupBy(identity, Accumulator.count)))

      for {
        g <- grouped.stream.toList
        f <- faceted.stream.toList
      } yield {
        g.toMap shouldBe Map("a" -> (2L, 2.0), "b" -> (1L, 1.0), "c" -> (3L, 3.0))
        f.head.left shouldBe List(6L)
        f.head.right.toMap shouldBe Map("a" -> 2L, "b" -> 1L, "c" -> 3L)
      }
    }

    "support groupBy3 (three accumulators)" in {
      val base = Pipeline.from(Stream.emits(List(1, 2, 3, 4, 5, 6)))
      val grouped = base.pipe(Stage.groupBy3[Int, String, Long, Long, Double, Double, Option[Int], Option[Int]](
        key = n => if (n % 2 == 0) "even" else "odd",
        a1 = Accumulator.count,
        a2 = Accumulator.sum(_.toDouble),
        a3 = Accumulator.max(identity)
      ))
      grouped.stream.toList.map { rows =>
        rows.toMap shouldBe Map(
          "odd" -> (3L, 9.0, Some(5)),
          "even" -> (3L, 12.0, Some(6))
        )
      }
    }
  }
}


