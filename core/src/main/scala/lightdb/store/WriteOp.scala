package lightdb.store

import lightdb.doc.Document
import lightdb.id.Id

sealed trait WriteOp[Doc <: Document[Doc]]

object WriteOp {
  case class Insert[Doc <: Document[Doc]](doc: Doc) extends WriteOp[Doc] {
    override def toString: String = s"Insert(${doc._id})"
  }
  case class Upsert[Doc <: Document[Doc]](doc: Doc) extends WriteOp[Doc] {
    override def toString: String = s"Upsert(${doc._id})"
  }
  case class Delete[Doc <: Document[Doc]](id: Id[Doc]) extends WriteOp[Doc] {
    override def toString: String = s"Delete($id)"
  }
}