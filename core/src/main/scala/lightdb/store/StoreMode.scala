package lightdb.store

import lightdb.doc.{Document, DocumentModel}

sealed trait StoreMode[Doc <: Document[Doc], +Model <: DocumentModel[Doc]] {
  def isAll: Boolean = false
  def isIndexes: Boolean = false
}

object StoreMode {
  case class All[Doc <: Document[Doc], Model <: DocumentModel[Doc]]() extends StoreMode[Doc, Model] {
    override def isAll: Boolean = true
  }
  case class Indexes[Doc <: Document[Doc], Model <: DocumentModel[Doc]](storage: Store[Doc, Model]) extends StoreMode[Doc, Model] {
    override def isIndexes: Boolean = true
  }
}