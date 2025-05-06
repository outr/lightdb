package lightdb.graph

import lightdb.doc.Document
import lightdb.id.{EdgeId, Id}

import scala.language.implicitConversions

trait EdgeDocument[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]] extends Document[Doc] {
  def _id: EdgeId[Doc, From, To]
  def _from: Id[From]
  def _to: Id[To]
}