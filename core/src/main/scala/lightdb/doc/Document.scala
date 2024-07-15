package lightdb.doc

import lightdb.Id

trait Document[Doc <: Document[Doc]] {
  def _id: Id[Doc]
}
