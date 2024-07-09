package benchmark.bench

import fabric.rw._

case class StatusLog(progress: Double,
                     timeStamp: Long,
                     elapsed: Double,
                     heap: Long,
                     nonHeap: Long,
                     cpuLoad: Double,
                     cpuTime: Long)

object StatusLog {
  implicit val rw: RW[StatusLog] = RW.gen
}