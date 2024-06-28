package lightdb.document

import lightdb.Id

trait Document[D <: Document[D]] {
  def _id: Id[D]
}