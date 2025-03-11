package lightdb.doc.graph

import lightdb.Id
import lightdb.doc.Document

import scala.language.implicitConversions

trait EdgeDocument[Doc <: Document[Doc], From <: Document[From], To <: Document[To]] extends Document[Doc] {
  def _from: Id[From]
  def _to: Id[To]
}