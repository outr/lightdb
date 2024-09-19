package lightdb

import fabric.Json
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field

case class KeyValue(_id: Id[KeyValue], json: Json) extends Document[KeyValue]

object KeyValue extends DocumentModel[KeyValue] with JsonConversion[KeyValue] {
  override implicit val rw: RW[KeyValue] = RW.gen

  val json: Field[KeyValue, Json] = field("json", (doc: KeyValue) => doc.json)
}