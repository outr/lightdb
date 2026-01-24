package lightdb.progress

import rapid.Task
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

    override def apply(value: Option[Double], message: Option[String]): Unit = synchronized {
      val now = System.currentTimeMillis()
      current match {
        case Some(_) => current = Some(Progress(value, message))
        case None if lastProgress + delayMS < now =>
          progressManager(value, message)
          lastProgress = now
        case None =>
          val delay = (lastProgress + delayMS) - now
          current = Some(Progress(value, message))
          Task.sleep(delay.millis).function {
            pm.synchronized {
              current.foreach { p =>
                progressManager(p.value, p.message)
              }
              current = None
              lastProgress = System.currentTimeMillis()
            }
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