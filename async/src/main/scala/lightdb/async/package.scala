package lightdb

import lightdb.collection.Collection
import lightdb.doc.DocModel

import scala.language.implicitConversions

package object async {
  implicit def collection2Extras[Doc, Model <: DocModel[Doc]](collection: Collection[Doc, Model]): CollectionExtras[Doc, Model] =
    CollectionExtras(collection)
}
