package lightdb.index

import fabric.rw.{Convertible, RW}
import fabric.{Json, Null}
import lightdb.Document
import lightdb.model.{AbstractCollection, Collection}

trait IndexedField[F, D <: Document[D]] {
  implicit def rw: RW[F]

  def fieldName: String
  def indexSupport: IndexSupport[D]
  def get: D => List[F]
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)

  indexSupport.index.register(this)
}