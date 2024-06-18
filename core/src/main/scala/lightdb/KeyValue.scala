package lightdb

import fabric.Json
import fabric.rw._
import lightdb.document.{Document, DocumentModel}

case class KeyValue(_id: Id[KeyValue], value: Json) extends Document[KeyValue]

object KeyValue extends DocumentModel[KeyValue] {
  implicit val rw: RW[KeyValue] = RW.gen
}