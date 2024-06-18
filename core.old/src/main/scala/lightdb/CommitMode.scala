package lightdb

sealed trait CommitMode

object CommitMode {
  case object Auto extends CommitMode
  case object Manual extends CommitMode
  case object Async extends CommitMode
}