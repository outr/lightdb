package lightdb

import fabric.Json
import fabric.rw._

case class KeyValue(_id: Id[KeyValue], value: Json) extends Document[KeyValue]

object KeyValue {
  implicit val rw: RW[KeyValue] = RW.gen
}