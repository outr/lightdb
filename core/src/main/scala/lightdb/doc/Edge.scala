package lightdb.doc

import lightdb.Id
import lightdb.collection.Collection
import rapid.Task

trait Edge[Doc <: Document[Doc], From <: Document[Doc], To <: Document[Doc]] extends Document[Doc] {
  def _from: Id[From]
  def _to: Id[To]
}

trait EdgeModel[Doc <: Document[Doc], From <: Document[Doc], To <: Document[Doc]] extends DocumentModel[Doc] {
  override def init[Model <: DocumentModel[Doc]](collection: Collection[Doc, Model]): Task[Unit] = {
    super.init(collection)
  }
}