package lightdb.doc

import lightdb.util.Unique
import lightdb.{Field, Id}

trait DocumentModel[Doc <: Document[Doc]] extends DocModel[Doc] {
  val _id: Field.Unique[Doc, Id[Doc]] = field.unique("_id", _._id)

  def id(value: String = Unique()): Id[Doc] = Id(value)
}
