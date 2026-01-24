package lightdb.field

import fabric.*
import lightdb.doc.Document

case class FieldAndValue[Doc <: Document[Doc], V](field: Field[Doc, V], value: V) {
  lazy val json: Json = field.rw.read(value)

  def update(json: Json): Json = json.merge(obj(
    field.name -> this.json
  ))
}