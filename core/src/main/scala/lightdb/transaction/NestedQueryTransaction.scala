package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}

trait NestedQueryTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends CollectionTransaction[Doc, Model]

