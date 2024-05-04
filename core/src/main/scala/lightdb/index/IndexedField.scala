package lightdb.index

import fabric.rw.{Convertible, RW}
import fabric.{Json, Null}
import lightdb.Document
import lightdb.model.Collection

trait IndexedField[F, D <: Document[D]] {
  implicit def rw: RW[F]

  def fieldName: String
  def collection: Collection[D]
  def get: D => Option[F]
  def getJson: D => Json = (doc: D) => get(doc) match {
    case Some(value) => value.json
    case None => Null
  }

  collection.asInstanceOf[IndexSupport[D]].index.register(this)
}