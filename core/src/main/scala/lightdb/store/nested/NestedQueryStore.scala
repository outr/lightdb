package lightdb.store.nested

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Collection

trait NestedQueryStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] { this: Collection[Doc, Model] =>
  override def supportsNestedQueries: Boolean = true
}

