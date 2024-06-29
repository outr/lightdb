package benchmark.bench

import fabric.rw.RW

case class BenchmarkReport(name: String, maxProgress: Double, logs: List[StatusLog])

object BenchmarkReport {
  implicit val rw: RW[BenchmarkReport] = RW.gen
}