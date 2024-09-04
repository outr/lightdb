package lightdb.filter

import fabric.rw.RW

trait Condition

object Condition {
  implicit val rw: RW[Condition] = RW.enumeration(List(Must, MustNot, Filter, Should))

  case object Must extends Condition
  case object MustNot extends Condition
  case object Filter extends Condition
  case object Should extends Condition
}