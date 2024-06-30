package benchmark.bench

import fabric.rw.RW

case class BenchmarkReport(benchName: String,
                           name: String,
                           maxProgress: Double,
                           size: Long,
                           logs: List[StatusLog])

object BenchmarkReport {
  implicit val rw: RW[BenchmarkReport] = RW.gen
}