package lightdb.field

import fabric._
import lightdb.doc.Document

case class FieldAndValue[Doc <: Document[Doc], V](field: Field[Doc, V], value: V) {
  def update(json: Json): Json = json.merge(obj(
    field.name -> field.rw.read(value)
  ))
}