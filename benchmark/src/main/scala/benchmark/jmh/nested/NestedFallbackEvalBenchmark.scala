package benchmark.jmh.nested

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.{Filter, NestedQuerySupport}
import lightdb.filter.FilterExtras
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
class NestedFallbackEvalState {
  @Param(Array("4", "16", "64"))
  var attrsPerDoc: Int = _

  @Param(Array("1000"))
  var docCount: Int = _

  private var docs: Vector[Entry] = Vector.empty
  private val filter = Entry.attrs.nested { attrs =>
    attrs.key === "tract-a" && attrs.percent >= 0.5
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    docs = Vector.tabulate(docCount) { i =>
      val attrs = Vector.tabulate(attrsPerDoc) { j =>
        Attr(
          key = if j % 5 == 0 then "tract-a" else s"tract-${j % 9}",
          percent = (j % 10) / 10.0
        )
      }.toList
      Entry(s"doc-$i", attrs)
    }
  }

  def evaluateAll(): Int = {
    var matched = 0
    docs.foreach { d =>
      if NestedQuerySupport.eval(filter, Entry, d) then matched += 1
    }
    matched
  }
}

class NestedFallbackEvalBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  def eval(state: NestedFallbackEvalState): Int = state.evaluateAll()
}

case class Attr(key: String, percent: Double)
object Attr {
  implicit val rw: RW[Attr] = RW.gen
}

case class Entry(title: String, attrs: List[Attr]) extends Document[Entry]
object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
  override implicit val rw: RW[Entry] = RW.gen

  trait Attrs extends Nested[List[Attr]] {
    val key: NP[String]
    val percent: NP[Double]
  }

  val title = field.index(_.title)
  val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
}
