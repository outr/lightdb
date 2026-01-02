package lightdb.progress

import fabric.rw._

case class Progress(value: Option[Double] = None, message: Option[String] = None)

object Progress {
  implicit val rw: RW[Progress] = RW.gen

  def percentage(current: Int, total: Int, message: Option[String] = None): Progress = {
    val value = current.toDouble / total.toDouble
    apply(Some(value), message)
  }
}