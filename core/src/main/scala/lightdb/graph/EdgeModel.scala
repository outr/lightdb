package lightdb.graph

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id

import scala.language.implicitConversions

trait EdgeModel[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends DocumentModel[Doc] {
  val _from: UniqueIndex[Doc, Id[From]] = field.unique("_from", _._from)
  val _to: UniqueIndex[Doc, Id[To]] = field.unique("_to", _._to)
}
