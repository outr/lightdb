package lightdb.async

import lightdb.collection.Collection
import lightdb.doc.DocModel

case class CollectionExtras[Doc, Model <: DocModel[Doc]](collection: Collection[Doc, Model]) extends AnyVal {
  def async: AsyncCollection[Doc, Model] = AsyncCollection(collection)
}
