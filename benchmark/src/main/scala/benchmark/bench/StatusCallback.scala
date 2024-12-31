package benchmark.bench

import com.sun.management.OperatingSystemMXBean

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

case class StatusCallback(every: Long = 30_000L) {
  private val _progress = new AtomicInteger(0)

  def progress(delta: Int = 1): Unit = _progress.addAndGet(delta)

  def currentProgress: Int = _progress.get()

  def logs: List[StatusLog] = _logs.reverse

  private var keepAlive = true
  private val startTime = System.currentTimeMillis()
  private var _logs = List.empty[StatusLog]

  def start(): Unit = {
    val t = new Thread {
      setDaemon(true)

      override def run(): Unit = {
        while (keepAlive) {
          report()
          Thread.sleep(every)
        }
      }
    }
    t.start()
  }

  def finish(): Unit = {
    keepAlive = false
    report()
  }

  private def report(): Unit = {
    val now = System.currentTimeMillis()
    val elapsed = (now - startTime) / 1000.0
    val memory = ManagementFactory.getMemoryMXBean
    val heap = memory.getHeapMemoryUsage
    val nonHeap = memory.getNonHeapMemoryUsage
    val heapUsed = heap.getUsed
    val nonHeapUsed = nonHeap.getUsed
    val os = ManagementFactory.getPlatformMXBean(classOf[OperatingSystemMXBean])
    val cpuLoad = os.getProcessCpuLoad
    val cpuTime = os.getProcessCpuTime
    val log = StatusLog(
      progress = _progress.get(),
      timeStamp = now,
      elapsed = elapsed,
      heap = heapUsed,
      nonHeap = nonHeapUsed,
      cpuLoad = cpuLoad,
      cpuTime = cpuTime
    )
    _logs = log :: _logs
  }
}
