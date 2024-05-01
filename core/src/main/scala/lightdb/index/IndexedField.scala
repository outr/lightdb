package lightdb.index

import lightdb.Document
import lightdb.model.Collection

trait IndexedField[F, D <: Document[D]] {
  def fieldName: String
  def collection: Collection[D]
  def get: D => Option[F]

  collection.asInstanceOf[IndexSupport[D]].index.register(this)
}