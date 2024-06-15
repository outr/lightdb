package lightdb.collection

import lightdb.LightDB
import lightdb.document.{Document, DocumentModel}

case class Collection[D <: Document[D]](model: DocumentModel[D], db: LightDB)