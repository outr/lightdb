package lightdb

import fabric.define.DefType
import fabric.rw._

case class Timestamp(value: Long = System.currentTimeMillis()) extends AnyVal

object Timestamp {
  implicit val rw: RW[Timestamp] = RW.from(
    r = _.value.json,
    w = j => Timestamp(j.asLong),
    d = DefType.Int
  )
  implicit val numeric: Numeric[Timestamp] = Numeric[Long].map(Timestamp.apply)(_.value)
}