package lightdb.store.nested

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Collection
import lightdb.store.NestedQueryCapability

trait NestedQueryStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] { this: Collection[Doc, Model] =>
  override def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Fallback
}

