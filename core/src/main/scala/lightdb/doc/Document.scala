package lightdb.doc

import lightdb.id.Id

trait Document[Doc <: Document[Doc]] {
  def _id: Id[Doc]
}
