package lightdb.doc

import fabric.Json
import fabric.rw.Asable

trait JsonConversion[Doc <: Document[Doc]] extends DocumentModel[Doc] {
  def convertFromJson(json: Json): Doc = json.as[Doc]

  override def map2Doc(map: Map[String, Any]): Doc =
    throw new RuntimeException("Should not be used in favor of JsonDocConversion")
}