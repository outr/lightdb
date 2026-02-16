package lightdb.progress

import rapid.Task
import rapid.logger
import reactify.Var

import scala.concurrent.duration.{DurationLong, FiniteDuration}

trait ProgressManager {
  def apply(value: Option[Double] = None, message: Option[String] = None): Unit

  def percentage(current: Int, total: Int, message: Option[String] = None): Unit = if total <= 0 then {
    apply(None, message)
  } else {
    apply(
      value = Some(current.toDouble / total.toDouble),
      message = message
    )
  }

  def split(count: Int): Vector[ProgressManager] = {
    require(count > 0, s"count must be > 0, got $count")

    val lock = new AnyRef
    val values = Array.fill[Option[Double]](count)(None)
    var lastMessage: Option[String] = None

    def clamp01(d: Double): Double =
      if d < 0.0 then 0.0
      else if d > 1.0 then 1.0
      else d

    def recomputeAndEmit(): Unit = {
      val (outValue, outMessage) = lock.synchronized {
        val sum = values.iterator.map(_.getOrElse(0.0)).sum
        val overall = clamp01(sum / count.toDouble)
        (Some(overall), lastMessage)
      }
      apply(outValue, outMessage)
    }

    Vector.tabulate(count) { idx =>
      new ProgressManager {
        override def apply(value: Option[Double], message: Option[String]): Unit = {
          val shouldEmit = lock.synchronized {
            value.foreach(v => values(idx) = Some(clamp01(v)))
            message.filter(_.nonEmpty).foreach(m => lastMessage = Some(m))
            true
          }
          if shouldEmit then recomputeAndEmit()
        }
      }
    }
  }
}

object ProgressManager {
  case object none extends ProgressManager {
    override def apply(value: Option[Double], message: Option[String]): Unit = {}
  }

  def timeDelayed(delay: FiniteDuration, progressManager: ProgressManager): ProgressManager = new ProgressManager { pm =>
    private val delayMS = delay.toMillis
    private var lastProgress = 0L
    private var current = Option.empty[Progress]

    private def emit(progress: Progress): Unit = {
      try {
        progressManager(progress.value, progress.message)
      } catch {
        case t: Throwable =>
          logger.warn("ProgressManager.timeDelayed failed to emit progress update", t)
      }
    }

    override def apply(value: Option[Double], message: Option[String]): Unit = {
      val now = System.currentTimeMillis()
      var emitNow: Option[Progress] = None
      var scheduleDelayMs = 0L
      var schedule = false

      synchronized {
        current match {
          case Some(_) =>
            current = Some(Progress(value, message))
          case None if lastProgress + delayMS < now =>
            val progress = Progress(value, message)
            lastProgress = now
            emitNow = Some(progress)
          case None =>
            scheduleDelayMs = (lastProgress + delayMS) - now
            current = Some(Progress(value, message))
            schedule = true
        }
      }

      emitNow.foreach(emit)

      if (schedule) {
        Task.sleep(scheduleDelayMs.millis).function {
          val toEmit = pm.synchronized {
            val pending = current
            current = None
            lastProgress = System.currentTimeMillis()
            pending
          }
          toEmit.foreach(emit)
        }.start()
      }
    }
  }

  def apply(v: Var[Progress]): ProgressManager = new ProgressManager {
    override def apply(value: Option[Double], message: Option[String]): Unit = v @= Progress(
      value = value,
      message = message
    )
  }
}