package lightdb.index

import lightdb.{Collection, Document}

trait IndexedField[F, D <: Document[D]] {
  def fieldName: String
  def collection: Collection[D]
  def get: D => F
}