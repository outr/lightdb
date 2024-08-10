package lightdb.filter

trait Condition

object Condition {
  case object Must extends Condition
  case object MustNot extends Condition
  case object Filter extends Condition
  case object Should extends Condition
}