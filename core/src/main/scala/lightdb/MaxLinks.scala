package lightdb

sealed trait MaxLinks

object MaxLinks {
  case object NoMax extends MaxLinks

  case class OverflowError(max: Int = 1000) extends MaxLinks

  case class OverflowWarn(max: Int = 1000) extends MaxLinks

  case class OverflowTrim(max: Int = 1000) extends MaxLinks
}