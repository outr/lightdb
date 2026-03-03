package lightdb.doc

import fabric.{Json, Obj}
import fabric.rw.*
import fabric.JsonWrapper
import lightdb.id.Id

case class JsonDocument(_id: Id[JsonDocument] = JsonDocument.id(), json: Json = Obj.empty)
  extends Document[JsonDocument] with JsonWrapper

object JsonDocument {
  implicit val rw: RW[JsonDocument] = RW.gen

  def id(value: String): Id[JsonDocument] = Id(value)

  def id(): Id[JsonDocument] = Id()
}

class JsonDocumentModel(name: String)
    extends DocumentModel[JsonDocument] with JsonConversion[JsonDocument] {
  override lazy val modelName: String = name
  override implicit val rw: RW[JsonDocument] = JsonDocument.rw

  val json: F[Json] = field("json", _.json)

  def withField[V: RW](name: String): this.type = {
    field[V](name, (doc: JsonDocument) => doc.json(name).as[V])
    this
  }

  def withField[V: RW](name: String, extract: Json => V): this.type = {
    field[V](name, (doc: JsonDocument) => extract(doc.json))
    this
  }

  def withIndex[V: RW](name: String): this.type = {
    field.index[V](name, (doc: JsonDocument) => doc.json(name).as[V])
    this
  }

  def withIndex[V: RW](name: String, extract: Json => V): this.type = {
    field.index[V](name, (doc: JsonDocument) => extract(doc.json))
    this
  }
}

object JsonDocumentModel {
  def apply(modelName: String): JsonDocumentModel = new JsonDocumentModel(modelName)
}
